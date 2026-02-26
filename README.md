# Pylon Issue Handoff Bot

This service posts regional support issue handoff snapshots to Slack using Pylon issue data.
Designed for **Support Operation** workflows.

## What It Does
- Computes regional shift metrics (US / EMEA / APAC)
- Tracks FR SLA Pending tickets
- Detects open handoff tickets
- Posts formatted snapshots to Slack

## Runtime
- Node.js 18+
- Ubuntu VM
- systemd timers
- Slack Bot Token
- Pylon API Token

## Required Environment
```
PYLON_TOKEN=...
SLACK_BOT_TOKEN=...
SCAN_B_LOOKBACK_DAYS=30
```

## Run Manually
```
node handoff_snapshot.mjs us
node handoff_snapshot.mjs emea
node handoff_snapshot.mjs apac
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
- New tickets during shift
- FR SLA Pending counts
- Aged SLA tickets
- Open handoff tickets

## Owner
Support Automation / SRE
