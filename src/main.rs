use reqwest::{Client, StatusCode};
use serde_json::json;
use std::{env, time::Duration};
use tokio::{
    task::JoinSet,
    time::{self, Instant, MissedTickBehavior},
};

const BACKENDS: &[&str] = &[
    "https://birch.catenarymaps.org",
];

const PING_USER_IDS: &[&str] = &["204371573799518208"];

// How frequently each backend is health checked.
const CHECK_INTERVAL: Duration = Duration::from_secs(10);

// A backend must fail continuously for this long before it is considered down.
const FAILURE_CONFIRMATION_WINDOW: Duration = Duration::from_secs(30);

// How frequently Discord is notified while a backend remains down.
const ALERT_INTERVAL: Duration = Duration::from_secs(60);

// Maximum time a health check is allowed to take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let webhook_url = env::var("DISCORD_WEBHOOK_URL")
        .expect("DISCORD_WEBHOOK_URL environment variable is required");

    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent("catenary-backend-monitor/0.1")
        .build()?;

    println!("Starting Catenary backend monitor.");

    for url in BACKENDS {
        println!("Monitoring: {url}");
    }

    let mut tasks = JoinSet::new();

    for &url in BACKENDS {
        let client = client.clone();
        let webhook_url = webhook_url.clone();

        tasks.spawn(async move {
            monitor_backend(client, webhook_url, url).await;
        });
    }

    // These monitoring tasks are intended to run forever.
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            eprintln!("Monitoring task unexpectedly exited: {error}");
        }
    }

    Ok(())
}

async fn monitor_backend(client: Client, webhook_url: String, url: &'static str) {
    let mut was_down = false;
    let mut failure_started_at: Option<Instant> = None;
    let mut last_alert: Option<Instant> = None;

    let mut ticker = time::interval(CHECK_INTERVAL);

    // If the process gets delayed, don't rapidly run multiple missed checks.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        match check_backend(&client, url).await {
            Ok(status) if status.is_success() => {
                // A successful check breaks the continuous-failure window.
                failure_started_at = None;

                if was_down {
                    was_down = false;
                    last_alert = None;

                    println!("OK: {url} has recovered ({status})");

                    let message = format!(
                        "✅ **BACKEND RECOVERED**\n\
                         `{url}` is responding normally again.\n\
                         Status: `{status}`"
                    );

                    // Recovery message intentionally does not ping users.
                    if let Err(error) = send_webhook(&client, &webhook_url, &message, false).await {
                        eprintln!("Failed to send recovery webhook for {url}: {error}");
                    }
                }
            }

            Ok(status) => {
                let reason = format!("HTTP status {status}");

                handle_failure(
                    &client,
                    &webhook_url,
                    url,
                    &reason,
                    &mut failure_started_at,
                    &mut was_down,
                    &mut last_alert,
                )
                .await;
            }

            Err(error) => {
                let reason = error.to_string();

                handle_failure(
                    &client,
                    &webhook_url,
                    url,
                    &reason,
                    &mut failure_started_at,
                    &mut was_down,
                    &mut last_alert,
                )
                .await;
            }
        }
    }
}

async fn check_backend(client: &Client, url: &str) -> Result<StatusCode, reqwest::Error> {
    let response = client.get(url).send().await?;
    Ok(response.status())
}

async fn handle_failure(
    client: &Client,
    webhook_url: &str,
    url: &str,
    reason: &str,
    failure_started_at: &mut Option<Instant>,
    was_down: &mut bool,
    last_alert: &mut Option<Instant>,
) {
    let now = Instant::now();

    if !*was_down {
        match *failure_started_at {
            Some(first_failure_at) => {
                let down_for = now.duration_since(first_failure_at);

                if down_for < FAILURE_CONFIRMATION_WINDOW {
                    eprintln!(
                        "SUSPECTED DOWN: {url} - {reason} \
                         (failing for {}s; waiting for 30s confirmation)",
                        down_for.as_secs()
                    );
                    return;
                }
            }
            None => {
                *failure_started_at = Some(now);

                eprintln!(
                    "SUSPECTED DOWN: {url} - {reason} \
                     (starting 30s confirmation window)"
                );
                return;
            }
        }

        *was_down = true;
        *failure_started_at = None;
    }

    let alert_due = match *last_alert {
        Some(previous_alert) => now.duration_since(previous_alert) >= ALERT_INTERVAL,
        None => true,
    };

    if !alert_due {
        return;
    }

    // Set this before sending so a broken Discord webhook doesn't cause
    // attempts every 10 seconds.
    *last_alert = Some(now);

    eprintln!("DOWN: {url} - {reason}");

    let mentions = PING_USER_IDS
        .iter()
        .map(|id| format!("<@{id}>"))
        .collect::<Vec<_>>()
        .join(" ");

    let message = format!(
        "{mentions}\n\
         🚨 **BACKEND DOWN**\n\
         URL: `{url}`\n\
         Reason: `{reason}`\n\
         Will notify again in 1 minute if it remains down."
    );

    if let Err(error) = send_webhook(client, webhook_url, &message, true).await {
        eprintln!("Failed to send Discord webhook for {url}: {error}");
    }
}

async fn send_webhook(
    client: &Client,
    webhook_url: &str,
    content: &str,
    allow_user_pings: bool,
) -> Result<(), reqwest::Error> {
    let allowed_users: &[&str] = if allow_user_pings { PING_USER_IDS } else { &[] };

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
