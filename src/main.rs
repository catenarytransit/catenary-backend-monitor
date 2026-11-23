use reqwest::{Client, header::CACHE_CONTROL};
use serde_json::{Value, json};
use std::{env, error::Error, time::Duration};
use tokio::{
    task::JoinSet,
    time::{self, Instant, MissedTickBehavior},
};

#[derive(Clone, Copy)]
enum ExpectedBody {
    BirchMicrotime,
    EdelweissHealth,
}

struct Backend {
    name: &'static str,
    url: &'static str,
    expected_body: ExpectedBody,
}

const BACKENDS: &[Backend] = &[
    Backend {
        name: "Birch",
        url: "https://birch.catenarymaps.org/microtime",
        expected_body: ExpectedBody::BirchMicrotime,
    },
    Backend {
        name: "Edelweiss",
        url: "https://edelweiss.catenarymaps.org/health",
        expected_body: ExpectedBody::EdelweissHealth,
    },
];

const PING_USER_IDS: &[&str] = &["204371573799518208"];

const CHECK_INTERVAL: Duration = Duration::from_secs(10);
const FAILURE_CONFIRMATION_WINDOW: Duration = Duration::from_secs(30);
const MIN_CONSECUTIVE_FAILURES: u32 = 3;
const BACKEND_ALERT_INTERVAL: Duration = Duration::from_secs(15 * 60);
const PROBE_WARNING_INTERVAL: Duration = Duration::from_secs(60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Debug)]
enum CheckOutcome {
    Healthy,
    // A response from the target returned a 5xx status.
    ServerError(String),
    // A failed request, unexpected response, or invalid health response does not
    // prove that the application is down. It may be a problem with this monitor.
    ProbeError(String),
}

impl CheckOutcome {
    fn reason(&self) -> Option<&str> {
        match self {
            Self::Healthy => None,
            Self::ServerError(reason) | Self::ProbeError(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureKind {
    Server,
    Probe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MonitorMode {
    Healthy,
    Down,
    ProbeUncertain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Notification {
    Down,
    Recovered,
    ProbeUncertain,
    ProbeRestored,
}

struct MonitorState {
    mode: MonitorMode,
    failure_streak: Option<(FailureKind, Instant, u32)>,
    last_notification: Option<Instant>,
}

impl Default for MonitorState {
    fn default() -> Self {
        Self {
            mode: MonitorMode::Healthy,
            failure_streak: None,
            last_notification: None,
        }
    }
}

impl MonitorState {
    fn observe(&mut self, outcome: &CheckOutcome, now: Instant) -> Option<Notification> {
        match outcome {
            CheckOutcome::Healthy => {
                self.failure_streak = None;
                self.last_notification = None;

                match std::mem::replace(&mut self.mode, MonitorMode::Healthy) {
                    MonitorMode::Healthy => None,
                    MonitorMode::Down => Some(Notification::Recovered),
                    MonitorMode::ProbeUncertain => Some(Notification::ProbeRestored),
                }
            }
            CheckOutcome::ServerError(_) => self.observe_failure(FailureKind::Server, now),
            CheckOutcome::ProbeError(_) => self.observe_failure(FailureKind::Probe, now),
        }
    }

    fn observe_failure(&mut self, kind: FailureKind, now: Instant) -> Option<Notification> {
        // Mixing a 5xx response with a DNS/TLS/timeout error is not enough
        // evidence to classify the whole interval as an application outage.
        let (started_at, count) = match self.failure_streak {
            Some((previous_kind, started_at, count)) if previous_kind == kind => {
                (started_at, count.saturating_add(1))
            }
            _ => (now, 1),
        };
        self.failure_streak = Some((kind, started_at, count));

        let (mode, notification, reminder_interval) = match kind {
            FailureKind::Server => (
                MonitorMode::Down,
                Notification::Down,
                BACKEND_ALERT_INTERVAL,
            ),
            FailureKind::Probe => (
                MonitorMode::ProbeUncertain,
                Notification::ProbeUncertain,
                PROBE_WARNING_INTERVAL,
            ),
        };

        if self.mode == mode {
            if self
                .last_notification
                .is_some_and(|last| now.duration_since(last) < reminder_interval)
            {
                return None;
            }
        } else if count < MIN_CONSECUTIVE_FAILURES
            || now.duration_since(started_at) < FAILURE_CONFIRMATION_WINDOW
        {
            return None;
        }

        self.mode = mode;
        self.last_notification = Some(now);
        Some(notification)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let webhook_url = env::var("DISCORD_WEBHOOK_URL")
        .expect("DISCORD_WEBHOOK_URL environment variable is required");

    let client = Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .user_agent("catenary-backend-monitor/0.2")
        .build()?;

    println!("Starting Catenary backend monitor.");
    let mut tasks = JoinSet::new();

    for backend in BACKENDS {
        println!("Monitoring {}: {}", backend.name, backend.url);
        let client = client.clone();
        let webhook_url = webhook_url.clone();

        tasks.spawn(async move {
            monitor_backend(client, webhook_url, backend).await;
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(()) => eprintln!("A backend monitor exited unexpectedly"),
            Err(error) => eprintln!("A backend monitor failed: {error}"),
        }
    }

    Ok(())
}

async fn monitor_backend(client: Client, webhook_url: String, backend: &'static Backend) {
    let mut state = MonitorState::default();
    let mut ticker = time::interval(CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        let outcome = check_backend(&client, backend).await;
        if let Some(reason) = outcome.reason() {
            eprintln!("{} health check: {reason}", backend.name);
        }

        if let Some(notification) = state.observe(&outcome, Instant::now()) {
            let (message, ping_users) = notification_message(backend, &outcome, notification);

            if let Err(error) = send_webhook(&client, &webhook_url, &message, ping_users).await {
                eprintln!(
                    "Failed to send Discord notification for {}: {}",
                    backend.name,
                    describe_error(&error)
                );
            }
        }
    }
}

async fn check_backend(client: &Client, backend: &Backend) -> CheckOutcome {
    let response = match client
        .get(backend.url)
        .header(CACHE_CONTROL, "no-cache")
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return CheckOutcome::ProbeError(format!(
                "HTTP request failed: {}",
                describe_error(&error)
            ));
        }
    };

    let status = response.status();
    if status.is_server_error() {
        return CheckOutcome::ServerError(format!("HTTP status {status}"));
    }
    if !status.is_success() {
        return CheckOutcome::ProbeError(format!("unexpected HTTP status {status}"));
    }

    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            return CheckOutcome::ProbeError(format!(
                "could not read health response: {}",
                describe_error(&error)
            ));
        }
    };

    if valid_health_body(backend.expected_body, &body) {
        CheckOutcome::Healthy
    } else {
        CheckOutcome::ProbeError(format!(
            "HTTP {status} but the {} health response did not match the expected payload",
            backend.name
        ))
    }
}

fn valid_health_body(expected: ExpectedBody, body: &str) -> bool {
    match expected {
        ExpectedBody::BirchMicrotime => body.trim().parse::<u128>().is_ok(),
        ExpectedBody::EdelweissHealth => match serde_json::from_str::<Value>(body) {
            Ok(value) => {
                value.get("service").and_then(Value::as_str) == Some("edelweiss")
                    && value.get("status").and_then(Value::as_str) == Some("ok")
            }
            Err(_) => false,
        },
    }
}

fn describe_error(error: &dyn Error) -> String {
    // reqwest's top-level error ("error sending request for url") is not
    // actionable. Include the source chain to expose DNS, TLS and timeout
    // failures without incorrectly attributing them to the backend.
    let mut description = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        description.push_str(": ");
        description.push_str(&cause.to_string());
        source = cause.source();
    }
    description
}

fn notification_message(
    backend: &Backend,
    outcome: &CheckOutcome,
    notification: Notification,
) -> (String, bool) {
    let reason = outcome.reason().unwrap_or("unknown reason");

    match notification {
        Notification::Down => {
            let mentions = PING_USER_IDS
                .iter()
                .map(|id| format!("<@{id}>"))
                .collect::<Vec<_>>()
                .join(" ");

            (
                format!(
                    "{mentions}\n\
                     🚨 **BACKEND DOWN**\n\
                     Service: **{}**\n\
                     URL: \x60{}\x60\n\
                     Reason: \x60{reason}\x60\n\
                     Confirmed repeated HTTP 5xx responses for at least 30 seconds.\n\
                     Will remind in 15 minutes if it remains down.",
                    backend.name, backend.url
                ),
                true,
            )
        }
        Notification::Recovered => (
            format!(
                "✅ **BACKEND RECOVERED**\n\
                 Service: **{}**\n\
                 URL: \x60{}\x60\n\
                 The application health response is valid again.",
                backend.name, backend.url
            ),
            false,
        ),
        Notification::ProbeUncertain => (
            format!(
                "⚠️ **HEALTH CHECK INCONCLUSIVE**\n\
                 Service: **{}**\n\
                 URL: \x60{}\x60\n\
                 Reason: \x60{reason}\x60\n\
                 The monitor could not verify health for at least 30 seconds. \
                 A DNS, TLS, network, proxy, or HTTP client error is not proof the backend is down.\n\
                 This warning does not ping users; the next reminder is in 1 hour.",
                backend.name, backend.url
            ),
            false,
        ),
        Notification::ProbeRestored => (
            format!(
                "✅ **HEALTH CHECK RESTORED**\n\
                 Service: **{}**\n\
                 URL: \x60{}\x60\n\
                 The monitor can verify the application health response again.",
                backend.name, backend.url
            ),
            false,
        ),
    }
}

async fn send_webhook(
    client: &Client,
    webhook_url: &str,
    content: &str,
    allow_user_pings: bool,
) -> Result<(), reqwest::Error> {
    let allowed_users: &[&str] = if allow_user_pings {
        PING_USER_IDS
    } else {
        &[]
    };

    let payload = json!({
        "username": "Catenary Backend Monitor",
        "content": content,
        "allowed_mentions": {
            "parse": [],
            "users": allowed_users
        }
    });

    client
        .post(webhook_url)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_error() -> CheckOutcome {
        CheckOutcome::ServerError("HTTP 503".to_string())
    }

    fn transport_error() -> CheckOutcome {
        CheckOutcome::ProbeError("DNS resolution failed".to_string())
    }

    #[test]
    fn validates_the_application_responses() {
        assert!(valid_health_body(
            ExpectedBody::BirchMicrotime,
            "1795467600123456"
        ));
        assert!(!valid_health_body(ExpectedBody::BirchMicrotime, "<html>OK</html>"));
        assert!(valid_health_body(
            ExpectedBody::EdelweissHealth,
            r#"{"service":"edelweiss","status":"ok"}"#
        ));
        assert!(!valid_health_body(
            ExpectedBody::EdelweissHealth,
            r#"{"service":"edelweiss","status":"unhealthy"}"#
        ));
        assert!(!valid_health_body(ExpectedBody::EdelweissHealth, "OK"));
    }

    #[test]
    fn needs_a_confirmed_server_failure_and_limits_reminders() {
        let start = Instant::now();
        let mut state = MonitorState::default();

        for seconds in [0, 10, 20] {
            assert_eq!(
                state.observe(&server_error(), start + Duration::from_secs(seconds)),
                None
            );
        }
        assert_eq!(
            state.observe(&server_error(), start + Duration::from_secs(30)),
            Some(Notification::Down)
        );
        assert_eq!(
            state.observe(&server_error(), start + Duration::from_secs(40)),
            None
        );
        assert_eq!(
            state.observe(&server_error(), start + Duration::from_secs(930)),
            Some(Notification::Down)
        );
        assert_eq!(
            state.observe(&CheckOutcome::Healthy, start + Duration::from_secs(940)),
            Some(Notification::Recovered)
        );
        assert_eq!(
            state.observe(&CheckOutcome::Healthy, start + Duration::from_secs(950)),
            None
        );
    }

    #[test]
    fn a_network_failure_never_pages_users_or_claims_the_backend_is_down() {
        let start = Instant::now();
        let mut state = MonitorState::default();

        for seconds in [0, 10, 20] {
            assert_eq!(
                state.observe(&transport_error(), start + Duration::from_secs(seconds)),
                None
            );
        }
        let notification = state.observe(&transport_error(), start + Duration::from_secs(30));
        assert_eq!(notification, Some(Notification::ProbeUncertain));

        let (_, ping_users) = notification_message(
            &BACKENDS[0],
            &transport_error(),
            notification.unwrap(),
        );
        assert!(!ping_users);
        assert_eq!(
            state.observe(&transport_error(), start + Duration::from_secs(90)),
            None
        );
        assert_eq!(
            state.observe(&transport_error(), start + Duration::from_secs(3630)),
            Some(Notification::ProbeUncertain)
        );
        assert_eq!(
            state.observe(&CheckOutcome::Healthy, start + Duration::from_secs(3640)),
            Some(Notification::ProbeRestored)
        );
    }

    #[test]
    fn mixed_failure_types_and_successes_reset_confirmation() {
        let start = Instant::now();
        let mut state = MonitorState::default();

        for (seconds, outcome) in [
            (0, server_error()),
            (10, transport_error()),
            (20, server_error()),
            (30, transport_error()),
            (40, server_error()),
            (50, CheckOutcome::Healthy),
        ] {
            assert_eq!(
                state.observe(&outcome, start + Duration::from_secs(seconds)),
                None
            );
        }

        for seconds in [60, 70, 80] {
            assert_eq!(
                state.observe(&server_error(), start + Duration::from_secs(seconds)),
                None
            );
        }
        assert_eq!(
            state.observe(&server_error(), start + Duration::from_secs(90)),
            Some(Notification::Down)
        );
    }

    #[test]
    fn sustained_probe_failures_replace_a_previous_down_classification() {
        let start = Instant::now();
        let mut state = MonitorState::default();

        for seconds in [0, 10, 20, 30] {
            state.observe(&server_error(), start + Duration::from_secs(seconds));
        }
        assert_eq!(state.mode, MonitorMode::Down);

        for seconds in [40, 50, 60] {
            assert_eq!(
                state.observe(&transport_error(), start + Duration::from_secs(seconds)),
                None
            );
        }
        assert_eq!(
            state.observe(&transport_error(), start + Duration::from_secs(70)),
            Some(Notification::ProbeUncertain)
        );
        assert_eq!(state.mode, MonitorMode::ProbeUncertain);
    }
}
