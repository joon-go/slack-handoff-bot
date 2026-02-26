# On‑Call Runbook

## Immediate Re‑Run
```
systemctl start handoff-emea.service
```

## View Last Output
```
journalctl -u handoff-emea.service -n 50
```

## Rollback
```
git checkout previous_commit
systemctl restart handoff-emea.timer
```
