use {
    crate::prom::GprcMessageKind,
    prometheus::{Gauge, GaugeVec, IntCounterVec, Opts},
};

lazy_static::lazy_static! {
    pub(crate) static ref GOOGLE_PUBSUB_RECV_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_recv_total", "Total number of received messages from gRPC by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_sent_total", "Total number of uploaded messages to pubsub by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_MESSAGES_QUEUE_SIZE: GaugeVec = GaugeVec::new(
        Opts::new("google_pubsub_messages_queue_size", "Total number of messages in the queue for sending to pubsub by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS: Gauge = Gauge::new(
        "google_pubsub_send_batches_in_progress", "Number of batches in progress"
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS: GaugeVec = GaugeVec::new(
        Opts::new("google_pubsub_awaiters_in_progress", "Number of awaiters in progress by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_drop_oversized_total", "Total number of dropped oversized messages"),
        &["kind"]
    ).unwrap();
}

pub fn recv_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_RECV_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_RECV_TOTAL.with_label_values(&["total"]).inc()
}

pub fn sent_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_SENT_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_SENT_TOTAL.with_label_values(&["total"]).inc()
}

pub fn messages_queue_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_MESSAGES_QUEUE_SIZE
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_MESSAGES_QUEUE_SIZE
        .with_label_values(&["total"])
        .inc()
}

pub fn messages_queue_dec(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_MESSAGES_QUEUE_SIZE
        .with_label_values(&[kind.as_str()])
        .dec();
    GOOGLE_PUBSUB_MESSAGES_QUEUE_SIZE
        .with_label_values(&["total"])
        .dec()
}

pub fn send_batches_inc() {
    GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS.inc()
}

pub fn send_batches_dec() {
    GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS.dec()
}

pub fn send_awaiters_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&["total"])
        .inc()
}

pub fn send_awaiters_dec(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&[kind.as_str()])
        .dec();
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&["total"])
        .dec()
}

pub fn drop_oversized_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc()
}
