use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    OrderSent,
    AckReceived,
    FillReceived,
    Timeout,
    Error,
}

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::OrderSent => "order_sent",
            EventKind::AckReceived => "ack_received",
            EventKind::FillReceived => "fill_received",
            EventKind::Timeout => "timeout",
            EventKind::Error => "error",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub run_id: String,
    pub bot_id: String,
    pub seq_no: u64,
    pub client_order_id: String,
    pub event_type: EventKind,
    pub send_ts_ns: u64,
    pub recv_ts_ns: u64,
    pub latency_ns: u64,
}

impl TelemetryEvent {
    pub fn into_proto(self) -> crate::proto::TelemetryEvent {
        crate::proto::TelemetryEvent {
            run_id: self.run_id,
            bot_id: self.bot_id,
            seq_no: self.seq_no,
            client_order_id: self.client_order_id,
            event_type: self.event_type.as_str().to_string(),
            send_ts_ns: self.send_ts_ns,
            recv_ts_ns: self.recv_ts_ns,
            latency_ns: self.latency_ns,
        }
    }
}
