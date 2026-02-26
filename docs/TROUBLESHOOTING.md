# Troubleshooting

## Slack Not Posting
- Check SLACK_BOT_TOKEN
- Confirm `chat:write` scope

## Pylon Rate Limits
- Script retries automatically
- Reduce paging window if needed

## Missing Handoff Tickets
Verify:
- Ticket is open
- Team = L1+L2
- `hand_off_region` is set

## Time Window Issues
- Luxon controls timezone
- Confirm Pacific Time logic
