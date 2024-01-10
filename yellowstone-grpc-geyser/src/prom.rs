use {
    crate::{config::ConfigPrometheus, version::VERSION as VERSION_INFO},
    futures::future::FutureExt,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    log::error,
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder},
    solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    std::sync::Once,
    tokio::sync::oneshot,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    pub static ref SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status", "Last processed slot by plugin"),
        &["status"]
    ).unwrap();

    pub static ref INVALID_FULL_BLOCKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("invalid_full_blocks_total", "Total number of fails on constructin full blocks"),
        &["reason"]
    ).unwrap();

    pub static ref MESSAGE_QUEUE_SIZE: IntGauge = IntGauge::new(
        "message_queue_size", "Size of geyser message queue"
    ).unwrap();

    pub static ref CONNECTIONS_TOTAL: IntGauge = IntGauge::new(
        "connections_total", "Total number of connections to GRPC service"
    ).unwrap();

    pub static ref CONNECTION_INFO: IntGaugeVec = IntGaugeVec::new(
        Opts::new("connection_info", "processed finalized slot for client"),
        &["id"]
    ).unwrap();
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown_signal: oneshot::Sender<()>,
}

impl PrometheusService {
    pub fn new(config: Option<ConfigPrometheus>) -> hyper::Result<Self> {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            macro_rules! register {
                ($collector:ident) => {
                    REGISTRY
                        .register(Box::new($collector.clone()))
                        .expect("collector can't be registered");
                };
            }
            register!(VERSION);
            register!(SLOT_STATUS);
            register!(INVALID_FULL_BLOCKS);
            register!(MESSAGE_QUEUE_SIZE);
            register!(CONNECTIONS_TOTAL);
            register!(CONNECTION_INFO);

            VERSION
                .with_label_values(&[
                    VERSION_INFO.buildts,
                    VERSION_INFO.git,
                    VERSION_INFO.package,
                    VERSION_INFO.proto,
                    VERSION_INFO.rustc,
                    VERSION_INFO.solana,
                    VERSION_INFO.version,
                ])
                .inc();
        });

        let (shutdown_signal, shutdown) = oneshot::channel();
        if let Some(ConfigPrometheus { address }) = config {
            let make_service = make_service_fn(move |_: &AddrStream| async move {
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| async move {
                    let response = match req.uri().path() {
                        "/metrics" => metrics_handler(),
                        _ => not_found_handler(),
                    };
                    Ok::<_, hyper::Error>(response)
                }))
            });
            let server = Server::try_bind(&address)?.serve(make_service);
            let shutdown = shutdown.map(|_| Ok(()));
            tokio::spawn(async move {
                if let Err(error) = tokio::try_join!(server, shutdown) {
                    error!("prometheus service failed: {}", error);
                }
            });
        }

        Ok(PrometheusService { shutdown_signal })
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_signal.send(());
    }
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder().body(Body::from(metrics)).unwrap()
}

fn not_found_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

pub fn update_slot_status(status: SlotStatus, slot: u64) {
    SLOT_STATUS
        .with_label_values(&[match status {
            SlotStatus::Processed => "processed",
            SlotStatus::Confirmed => "confirmed",
            SlotStatus::Rooted => "finalized",
        }])
        .set(slot as i64);
}

pub fn update_invalid_blocks(reason: impl AsRef<str>) {
    INVALID_FULL_BLOCKS
        .with_label_values(&[reason.as_ref()])
        .inc();
    INVALID_FULL_BLOCKS.with_label_values(&["all"]).inc();
}
