# Pylon Issue Handoff Bot

This service posts regional support issue handoff snapshots to Slack using Pylon issue data.
Designed for **Support Operation** workflows.

## What It Does
- Pulls issue data from the Pylon API via paginated search
- Computes regional shift metrics (US / EMEA / APAC)
- Tracks FR SLA Pending tickets (P0/P1 and P2/P3)
- Detects aged SLA tickets (> 1 week old)
- Detects open handoff tickets with region and meeting-required flags
- Tracks Discord community open issues
- Computes per-agent assignment breakdowns (Pylon vs Discord sources)
- Posts formatted snapshots to Slack (`#csorg-support-handoff`)

## How It Works

Uses a two-pass scan architecture:

- **Scan A** — Pages through Pylon issues to count tickets created during the shift window. Early-stops once it passes the window boundary.
- **Scan B** — Pages through issues (configurable lookback, default 30 days) to compute queue health: SLA pending counts, aged tickets, handoff issues, and Discord community open issues.

### Shift Windows (Pacific Time)
| Region | Window | Timer |
|--------|--------|-------|
| APAC | 18:00 – 03:00 (cross-midnight) | 03:00 PT |
| EMEA | 01:00 – 10:00 | 10:00 PT |
| US | 09:00 – 18:00 | 18:00 PT |

## Runtime
- Node.js 18+ (uses built-in `fetch`)
- Ubuntu VM
- systemd timers
- Slack Bot Token
- Pylon API Token
- Single dependency: [luxon](https://moment.github.io/luxon/) for timezone-safe date handling

## Setup
```
cd src
npm install
```

## Required Environment
```
PYLON_TOKEN=...          # Pylon API bearer token
SLACK_BOT_TOKEN=...      # Slack bot OAuth token
SCAN_B_LOOKBACK_DAYS=30  # How far back Scan B pages (days, default 30)
```

## Run Manually
```
node src/handoff_snapshot.mjs us
node src/handoff_snapshot.mjs emea
node src/handoff_snapshot.mjs apac
```

## systemd Operations
Check timers:
```
systemctl list-timers | grep handoff
```

Check logs:
```
journalctl -u handoff-emea.service -n 100
```

Restart:
```
systemctl restart handoff-emea.service
```

## Slack Output Includes
- New tickets created during the shift
- Per-agent assignment breakdown (Pylon and Discord sources)
- Discord community open issues count
- FR SLA Pending P0/P1 count with issue line items
- FR SLA Pending P2/P3 count
- FR SLA Pending aged > 1 week with issue line items
- Open handoff tickets with region, assignee, and meeting-required flag

## Owner
Support Automation / SRE
