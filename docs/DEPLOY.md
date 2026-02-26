# Deployment Guide (Ubuntu + systemd)

## Install Node
```
sudo apt install nodejs npm
```

## Copy Files
```
scp handoff_snapshot.mjs user@vm:~/scripts/
```

## Enable Services
```
sudo systemctl daemon-reload
sudo systemctl enable handoff-emea.timer
sudo systemctl start handoff-emea.timer
```

## Validate Run
```
systemctl start handoff-emea.service
```
