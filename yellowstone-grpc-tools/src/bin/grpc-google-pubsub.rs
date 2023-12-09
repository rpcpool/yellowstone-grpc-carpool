use {
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt},
    google_cloud_googleapis::pubsub::v1::PubsubMessage,
    google_cloud_pubsub::{client::Client, subscription::SubscriptionConfig},
    std::{net::SocketAddr, time::Duration},
    tokio::{task::JoinSet, time::sleep},
    tracing::{info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount},
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
        let client = config.create_client().await?;

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
        if !topic.exists(None).await? {
            anyhow::ensure!(
                config.create_if_not_exists,
                "topic {} doesn't exists",
                config.topic
            );
            topic.create(None, None).await?;
        }
        let publisher = topic.new_publisher(Some(config.get_publisher_config()));

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

        // Receive-send loop
        let mut send_tasks = JoinSet::new();
        let mut cached_messages: Vec<(PubsubMessage, GprcMessageKind)> = vec![];
        'outer: loop {
            let sleep = sleep(Duration::from_millis(config.bulk_max_wait_ms as u64));
            tokio::pin!(sleep);
            let mut messages_size = 0;
            let mut messages = vec![];
            let mut prom_kinds = vec![];
            'qwe: while messages.len() < config.bulk_max_size {
                while let Some((message, prom_kind)) = cached_messages.pop() {
                    if messages.len() + 1 < config.bulk_max_size
                        && messages_size + message.data.len() < 9_500_000
                    {
                        messages_size += message.data.len();
                        messages.push(message);
                        prom_kinds.push(prom_kind);
                    } else {
                        cached_messages.push((message, prom_kind));
                        break 'qwe;
                    }
                }

                let message = tokio::select! {
                    _ = &mut shutdown => break 'outer,
                    _ = &mut sleep => break,
                    maybe_result = send_tasks.join_next() => match maybe_result {
                        Some(result) => {
                            result??;
                            continue;
                        }
                        None => tokio::select! {
                            _ = &mut shutdown => break 'outer,
                            _ = &mut sleep => break,
                            message = geyser.next() => message,
                        }
                    },
                    message = geyser.next() => message,
                }
                .transpose()?;
                let message = match message {
                    Some(message) => message,
                    None => break 'outer,
                };

                match &message {
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(UpdateOneof::Ping(_)),
                    } => {}
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(UpdateOneof::Pong(_)),
                    } => {}
                    SubscribeUpdate {
                        filters,
                        update_oneof: Some(UpdateOneof::Block(block_message)),
                    } => {
                        // hack for now
                        let exclude: Vec<Vec<u8>> = [
                            // "11111111111111111111111111111111", // 926
                            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // 707
                            "Vote111111111111111111111111111111111111111", // 672
                                                                           // "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", // 140
                                                                           // "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f", // 120
                                                                           // "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH", // 74
                                                                           // "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH", // 35
                                                                           // "zDEXqXEG7gAyxb1Kg9mK5fPnUdENCGKzWrM21RMdWRq", // 33
                                                                           // "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // 28
                                                                           // "ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD", // 26
                                                                           // "SAGEqqFewepDHH6hMDcmWy7yjHPpyKLDnRXKb3Ki8e6", // 18
                                                                           // "Cargo8a1e6NkGyrjy4BQEW4ASGKs9KSyDyUrXMfpJoiH", // 18
                                                                           // "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX", // 17
                                                                           // "Stake11111111111111111111111111111111111111", // 14
                        ]
                        .into_iter()
                        .map(|v| bs58::decode(v).into_vec())
                        .collect::<Result<_, _>>()?;

                        for account in block_message.accounts.iter() {
                            if exclude.iter().any(|key| key == &account.owner) {
                                continue;
                            }

                            let update_oneof = UpdateOneof::Account(SubscribeUpdateAccount {
                                account: Some(account.clone()),
                                slot: block_message.slot,
                                is_startup: false,
                            });
                            let prom_kind = GprcMessageKind::from(&update_oneof);
                            cached_messages.push((
                                PubsubMessage {
                                    data: SubscribeUpdate {
                                        filters: filters.clone(),
                                        update_oneof: Some(update_oneof),
                                    }
                                    .encode_to_vec(),
                                    ..Default::default()
                                },
                                prom_kind,
                            ));
                        }
                    }
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: Some(value),
                    } => {
                        cached_messages.push((
                            PubsubMessage {
                                data: message.encode_to_vec(),
                                ..Default::default()
                            },
                            GprcMessageKind::from(value),
                        ));
                    }
                    SubscribeUpdate {
                        filters: _,
                        update_oneof: None,
                    } => unreachable!("Expect valid message"),
                };
            }
            if messages.is_empty() {
                continue;
            }

            for (awaiter, prom_kind) in publisher
                .publish_bulk(messages)
                .await
                .into_iter()
                .zip(prom_kinds.into_iter())
            {
                send_tasks.spawn(async move {
                    awaiter.get().await?;
                    prom::sent_inc(prom_kind);
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
        warn!("shutdown received...");
        while let Some(result) = send_tasks.join_next().await {
            result??;
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
    let config = config_load::<Config>(&args.config).await?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address)?;
    }

    args.action.run(config).await
}
