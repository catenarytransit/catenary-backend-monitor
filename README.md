# Catenary Backend Monitor

A lightweight Rust monitor that checks application-level HTTP responses and sends
state-change notifications to Discord.

## Monitored endpoints

| Service | URL | Expected response |
| --- | --- | --- |
| Birch | `https://birch.catenarymaps.org/microtime` | HTTP 2xx and a numeric microsecond timestamp |
| Edelweiss | `https://edelweiss.catenarymaps.org/health` | HTTP 2xx and JSON containing `{"service":"edelweiss","status":"ok"}` |

Birch's root page isn't a dedicated health check. The `/microtime` endpoint
is a lightweight application handler. Edelweiss provides its own `/health`
route. The checks establish that the respective HTTP handler is accessible;
they don't establish full end-to-end availability of every downstream dependency.

## Alert behavior

Checks are attempted every 10 seconds, with a 4-second connection timeout and
an 8-second total HTTP request timeout. At least three consecutive failures of
the same category spanning 30 seconds are necessary before entering a problem
state. Successful health checks reset the confirmation window.

- **BACKEND DOWN**: repeated HTTP 5xx responses for at least 30 seconds.
  Pings the configured Discord user IDs; reminders at most once every 15 minutes.
- **HEALTH CHECK INCONCLUSIVE**: repeated transport errors (DNS, TLS, network,
  connection timeouts) or invalid/unexpected health responses for at least 30
  seconds. Does not ping; reminders at most once every hour.
- **BACKEND RECOVERED** and **HEALTH CHECK RESTORED**: emitted once a valid
  health response is received following a confirmed problem.

The monitor intentionally does **not** assert that a backend is down when it
cannot connect to the backend: failures can be local to the monitor. The
underlying cause chain is logged to help diagnose DNS and TLS errors.
A 4xx response or a malformed success payload is a probe/configuration
problem, not a confirmed 5xx server error.

**Caveat:** A backend that is completely unreachable can also be genuinely
offline. With this conservative single-vantage-point policy, that scenario
produces an inconclusive warning, not a paged outage. For authoritative
unreachable-host paging, add a second independent monitoring vantage point or
an independent application signal before escalating.

## Run

Set `DISCORD_WEBHOOK_URL` to your Discord webhook URL (prefer a secret or
systemd EnvironmentFile; do not commit it). Then run:

```sh
cargo run --release
```

Test with:

```sh
cargo test
cargo fmt --all -- --check
```

If running under systemd, restart the monitor service after deploying the
updated executable. Monitor logs to see underlying causes of health failures.
