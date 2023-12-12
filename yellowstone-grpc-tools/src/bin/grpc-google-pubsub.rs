use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::{
        future::{pending, BoxFuture, FutureExt},
        stream::StreamExt,
    },
    google_cloud_googleapis::pubsub::v1::PubsubMessage,
    google_cloud_pubsub::{client::Client, subscription::SubscriptionConfig},
    std::{net::SocketAddr, time::Duration},
    tokio::{sync::mpsc, task::JoinSet, time::sleep},
    tracing::{debug, error, info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeUpdate, SubscribeUpdateAccount,
        },
        prost::Message as _,
    },
    yellowstone_grpc_tools::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        google_pubsub::{
            config::{Config, ConfigGrpc2PubSub},
            prom,
        },
        prom::{run_server as prometheus_run_server, GprcMessageKind},
        setup_tracing,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC Google Pub/Sub Tool")]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: String,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// Receive data from gRPC and send them to the Pub/Sub
    #[command(name = "grpc2pubsub")]
    Grpc2PubSub,
    /// Dev: subscribe to message from Pub/Sub and print them to Stdout
    #[command(name = "pubsub2stdout")]
    PubSub2Stdout {
        #[clap(long)]
        topic: String,
        #[clap(long)]
        subscription: String,
    },
    /// Dev: create Pub/Sub topic
    #[command(name = "pubsubTopicCreate")]
    PubSubTopicCreate { topic: String },
    /// Dev: delete Pub/Sub topic
    #[command(name = "pubsubTopicDelete")]
    PubSubTopicDelete { topic: String },
}

impl ArgsAction {
    async fn run(self, config: Config) -> anyhow::Result<()> {
        let shutdown = create_shutdown()?;
        let client = config.client.create_client().await?;

        match self {
            ArgsAction::Grpc2PubSub => {
                let config = config.grpc2pubsub.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2pubsub` section in config should be defined")
                })?;
                Self::grpc2pubsub(client, config, shutdown).await
            }
            ArgsAction::PubSub2Stdout {
                topic,
                subscription,
            } => Self::pubsub2stdout(client, topic, subscription, shutdown).await,
            ArgsAction::PubSubTopicCreate { topic } => {
                let topic = client.topic(&topic);
                if !topic.exists(None).await? {
                    topic.create(None, None).await?;
                }
                Ok(())
            }
            ArgsAction::PubSubTopicDelete { topic } => {
                client.topic(&topic).delete(None).await.map_err(Into::into)
            }
        }
    }

    async fn grpc2pubsub(
        client: Client,
        config: ConfigGrpc2PubSub,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        // Connect to Pub/Sub and create topic if not exists
        let topic = client.topic(&config.topic);
        if !topic
            .exists(None)
            .await
            .with_context(|| format!("failed to get topic: {}", config.topic))?
        {
            anyhow::ensure!(
                config.create_if_not_exists,
                "topic {} doesn't exists",
                config.topic
            );
            topic
                .create(None, None)
                .await
                .with_context(|| format!("failed to create topic: {}", config.topic))?;
        }
        let publisher = topic.new_publisher(Some(config.publisher.get_publisher_config()));

        // Create gRPC client & subscribe
        let mut client = GeyserGrpcClient::connect_with_timeout(
            config.endpoint,
            config.x_token,
            None,
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(5)),
            false,
        )
        .await?;
        let mut geyser = client.subscribe_once2(config.request.to_proto()).await?;

        // hack for now
        let exclude_pubkeys: Vec<Vec<u8>> = config
            .block_exclude_pubkeys
            .iter()
            .map(|v| bs58::decode(v).into_vec())
            .collect::<Result<_, _>>()?;
        let (messages_tx, mut messages_rx) = mpsc::unbounded_channel();
        let mut messages_jh = tokio::spawn(async move {
            while let Some(message) = geyser.next().await {
                let message = message.context("failed to get message from gRPC")?;
                match &message {
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(UpdateOneof::Ping(_)),
                    } => {
                        prom::recv_inc(GprcMessageKind::Ping);
                    }
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(UpdateOneof::Pong(_)),
                    } => {
                        prom::recv_inc(GprcMessageKind::Pong);
                    }
                    SubscribeUpdate {
                        filters,
                        update_oneof: Some(UpdateOneof::Block(block_message)),
                    } => {
                        for account in block_message.accounts.iter() {
                            if exclude_pubkeys.iter().any(|key| key == &account.owner) {
                                continue;
                            }

                            let update_oneof = UpdateOneof::Account(SubscribeUpdateAccount {
                                account: Some(account.clone()),
                                slot: block_message.slot,
                                is_startup: false,
                            });
                            let prom_kind = GprcMessageKind::from(&update_oneof);
                            messages_tx
                                .send((
                                    PubsubMessage {
                                        data: SubscribeUpdate {
                                            filters: filters.clone(),
                                            update_oneof: Some(update_oneof),
                                        }
                                        .encode_to_vec(),
                                        ..Default::default()
                                    },
                                    prom_kind,
                                ))
                                .context("failed to send gRPC message to pubsub queue")?;
                            prom::recv_inc(prom_kind);
                            prom::messages_queue_inc(prom_kind);
                        }
                    }
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(value),
                    } => {
                        if let UpdateOneof::Slot(slot) = value {
                            prom::set_slot_tip(
                                CommitmentLevel::try_from(slot.status).expect("valid commitment"),
                                slot.slot.try_into().expect("valid i64 slot"),
                            );
                        }

                        let prom_kind = GprcMessageKind::from(value);
                        messages_tx
                            .send((
                                PubsubMessage {
                                    data: message.encode_to_vec(),
                                    ..Default::default()
                                },
                                prom_kind,
                            ))
                            .context("failed to send gRPC message to pubsub queue")?;
                        prom::recv_inc(prom_kind);
                        prom::messages_queue_inc(prom_kind);
                    }
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: None,
                    } => unreachable!("Expect valid message"),
                }
            }
            Err::<(), anyhow::Error>(anyhow::anyhow!("gRPC stream finished"))
        });

        // Receive-send loop
        let mut send_tasks = JoinSet::new();
        let mut prefetched_message: Option<(PubsubMessage, GprcMessageKind)> = None;
        'receive_send_loop: loop {
            let sleep = sleep(config.batch.max_wait);
            tokio::pin!(sleep);

            let mut messages_size = 0;
            let mut messages = vec![];
            let mut prom_kinds = vec![];

            loop {
                if let Some((message, prom_kind)) = prefetched_message.take() {
                    if messages.len() < config.batch.max_messages
                        && messages_size + message.data.len() <= config.batch.max_size_bytes
                    {
                        messages_size += message.data.len();
                        messages.push(message);
                        prom_kinds.push(prom_kind);
                    } else if message.data.len() > config.batch.max_size_bytes {
                        prom::drop_oversized_inc(prom_kind);
                        debug!("drop {prom_kind:?} message, size: {}", message.data.len());
                    } else {
                        prefetched_message = Some((message, prom_kind));
                        break;
                    }
                }

                let send_task_fut = if send_tasks.is_empty() {
                    pending().boxed()
                } else {
                    send_tasks.join_next().boxed()
                };

                tokio::select! {
                    _ = &mut shutdown => break 'receive_send_loop,
                    messages_jh_result = &mut messages_jh => {
                        messages_jh_result??;
                        unreachable!();
                    }
                    _ = &mut sleep => break,
                    maybe_result = send_task_fut => match maybe_result {
                        Some(result) => {
                            prom::send_batches_dec();
                            result?;
                            continue;
                        }
                        None => unreachable!()
                    },
                    message = messages_rx.recv() => match message {
                        Some((message, prom_kind)) => {
                            prom::messages_queue_dec(prom_kind);
                            prefetched_message = Some((message, prom_kind));
                        }
                        None => {
                            messages_jh.await??;
                            anyhow::bail!("internal messages stream from gRPC is closed")
                        },
                    }
                };
            }
            if messages.is_empty() {
                continue;
            }

            while send_tasks.len() >= config.batch.max_in_progress {
                if let Some(result) = send_tasks.join_next().await {
                    prom::send_batches_dec();
                    result?;
                }
            }

            let awaiters = publisher.publish_bulk(messages).await;
            for prom_kind in prom_kinds.iter().copied() {
                prom::send_awaiters_inc(prom_kind);
            }
            send_tasks.spawn(async move {
                for (awaiter, prom_kind) in awaiters.into_iter().zip(prom_kinds.into_iter()) {
                    let status = if let Err(error) = awaiter.get().await {
                        error!("failed to send message {prom_kind:?}, error: {error:?}");
                        Err(())
                    } else {
                        Ok(())
                    };
                    prom::sent_inc(prom_kind, status);
                    prom::send_awaiters_dec(prom_kind);
                }
            });
            prom::send_batches_inc();
        }
        warn!("shutdown received...");
        while let Some(result) = send_tasks.join_next().await {
            prom::send_batches_dec();
            result?;
        }
        Ok(())
    }

    async fn pubsub2stdout(
        client: Client,
        topic: String,
        subscription: String,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        let topic = client.topic(&topic);
        if !topic.exists(None).await? {
            topic.create(None, None).await?;
        }
        let subscription = client.subscription(&subscription);
        if !subscription.exists(None).await? {
            let fqtn = topic.fully_qualified_name();
            let config = SubscriptionConfig::default();
            subscription.create(fqtn, config, None).await?;
        }

        let mut stream = subscription.subscribe(None).await?;
        loop {
            let msg = tokio::select! {
                _ = &mut shutdown => break,
                msg = stream.next() => match msg {
                    Some(msg) => msg,
                    None => break,
                }
            };

            msg.ack().await?;
            match SubscribeUpdate::decode(msg.message.data.as_ref()) {
                Ok(msg) => match msg.update_oneof {
                    Some(UpdateOneof::Account(msg)) => info!("#{}, account", msg.slot),
                    Some(UpdateOneof::Slot(msg)) => info!("#{}, slot", msg.slot),
                    Some(UpdateOneof::Transaction(msg)) => {
                        info!("#{}, transaction", msg.slot)
                    }
                    Some(UpdateOneof::Block(msg)) => info!("#{}, block", msg.slot),
                    Some(UpdateOneof::Ping(_)) => {}
                    Some(UpdateOneof::Pong(_)) => {}
                    Some(UpdateOneof::BlockMeta(msg)) => info!("#{}, blockmeta", msg.slot),
                    Some(UpdateOneof::Entry(msg)) => info!("#{}, entry", msg.slot),
                    None => {}
                },
                Err(error) => {
                    warn!("failed to decode message: {error}");
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    // Parse args
    let args = Args::parse();
    let config = config_load::<Config>(&args.config)
        .await
        .with_context(|| format!("failed to load config from file: {}", args.config))?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address)
            .with_context(|| format!("failed to run server at: {:?}", address))?;
    }

    args.action.run(config).await
}
