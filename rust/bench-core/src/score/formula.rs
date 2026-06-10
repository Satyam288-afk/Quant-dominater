use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default)]
pub struct ScoreInputs {
    pub valid: bool,
    pub p99_ms: f64,
    pub tps: f64,
    pub expected_tps: f64,
    pub orders_sent: i64,
    pub timeouts: i64,
    pub connect_errors: i64,
    pub cpu_pct: Option<f64>,
    pub mem_mb: Option<f64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct CompositeScore {
    pub final_score: f64,
    pub latency_score: f64,
    pub throughput_score: f64,
    pub stability_score: f64,
    pub resource_score: f64,
}

pub fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

pub fn latency_score(p99_ms: f64) -> f64 {
    if p99_ms <= 5.0 {
        return 100.0;
    }
    if p99_ms >= 100.0 {
        return 0.0;
    }
    round2(100.0 * (100.0 - p99_ms) / 95.0)
}

pub fn throughput_score(tps: f64, expected_tps: f64) -> f64 {
    if expected_tps <= 0.0 {
        return 100.0;
    }
    let mut score = 100.0 * tps / expected_tps;
    if score > 100.0 {
        score = 100.0;
    }
    if score < 0.0 {
        score = 0.0;
    }
    round2(score)
}

pub fn stability_score(orders_sent: i64, timeouts: i64, connect_errors: i64) -> f64 {
    if orders_sent <= 0 {
        if connect_errors > 0 {
            return 0.0;
        }
        return 100.0;
    }
    let bad = (timeouts + connect_errors) as f64;
    let mut score = 100.0 * (1.0 - bad / orders_sent as f64);
    if score < 0.0 {
        score = 0.0;
    }
    round2(score)
}

pub fn resource_efficiency_score(cpu_pct: Option<f64>, mem_mb: Option<f64>) -> f64 {
    // Until sandbox sampling lands we keep the same default as the Go scorer.
    let cpu = match cpu_pct {
        Some(v) if v > 0.0 => v.min(100.0),
        _ => return 100.0,
    };
    let mem = mem_mb.unwrap_or(0.0);
    // Soft linear penalty starting at 50% CPU / 512 MB.
    let cpu_penalty = ((cpu - 50.0).max(0.0)) * 1.5;
    let mem_penalty = ((mem - 512.0).max(0.0)) * 0.05;
    let score = (100.0 - cpu_penalty - mem_penalty).clamp(0.0, 100.0);
    round2(score)
}

pub fn compose(inputs: ScoreInputs) -> CompositeScore {
    if !inputs.valid {
        return CompositeScore::default();
    }
    let latency = latency_score(inputs.p99_ms);
    let throughput = throughput_score(inputs.tps, inputs.expected_tps);
    let stability = stability_score(inputs.orders_sent, inputs.timeouts, inputs.connect_errors);
    let resource = resource_efficiency_score(inputs.cpu_pct, inputs.mem_mb);
    let final_score =
        round2(0.40 * latency + 0.30 * throughput + 0.20 * stability + 0.10 * resource);
    CompositeScore {
        final_score,
        latency_score: latency,
        throughput_score: throughput,
        stability_score: stability,
        resource_score: resource,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_zeroes_everything() {
        let s = compose(ScoreInputs {
            valid: false,
            ..Default::default()
        });
        assert_eq!(s.final_score, 0.0);
    }

    #[test]
    fn matches_go_scorer_when_perfect() {
        let s = compose(ScoreInputs {
            valid: true,
            p99_ms: 2.0,
            tps: 1000.0,
            expected_tps: 1000.0,
            orders_sent: 1000,
            timeouts: 0,
            connect_errors: 0,
            cpu_pct: None,
            mem_mb: None,
        });
        assert_eq!(s.latency_score, 100.0);
        assert_eq!(s.throughput_score, 100.0);
        assert_eq!(s.stability_score, 100.0);
        assert_eq!(s.resource_score, 100.0);
        assert_eq!(s.final_score, 100.0);
    }

    #[test]
    fn latency_score_piecewise() {
        assert_eq!(latency_score(0.0), 100.0);
        assert_eq!(latency_score(5.0), 100.0);
        assert!(latency_score(50.0) > 0.0 && latency_score(50.0) < 100.0);
        assert_eq!(latency_score(100.0), 0.0);
        assert_eq!(latency_score(150.0), 0.0);
    }

    #[test]
    fn stability_handles_no_orders() {
        assert_eq!(stability_score(0, 0, 0), 100.0);
        assert_eq!(stability_score(0, 0, 1), 0.0);
        assert_eq!(stability_score(100, 10, 0), 90.0);
    }
}
