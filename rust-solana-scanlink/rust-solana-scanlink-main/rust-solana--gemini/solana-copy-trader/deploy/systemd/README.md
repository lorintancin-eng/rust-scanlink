## Rust Scanlink systemd rollout

Copy these files to the VPS:

- `deploy/systemd/rust-scanlink.service` -> `/etc/systemd/system/rust-scanlink.service`
- `deploy/systemd/rust-scanlink-janitor.service` -> `/etc/systemd/system/rust-scanlink-janitor.service`
- `deploy/systemd/rust-scanlink-janitor.timer` -> `/etc/systemd/system/rust-scanlink-janitor.timer`
- `deploy/systemd/rust-scanlink-healthcheck.service` -> `/etc/systemd/system/rust-scanlink-healthcheck.service`
- `deploy/systemd/rust-scanlink-healthcheck.timer` -> `/etc/systemd/system/rust-scanlink-healthcheck.timer`
- `ops/analytics_janitor.py` -> `/home/ubuntu/rust-scanlink/ops/analytics_janitor.py`
- `ops/scanner_healthcheck.sh` -> `/home/ubuntu/rust-scanlink/ops/scanner_healthcheck.sh`
- `ops/split_filter_db.py` -> `/home/ubuntu/rust-scanlink/ops/split_filter_db.py`

Recommended `.env` additions:

- `FILTER_DB_PATH=data/runtime.sqlite3`
- `ANALYTICS_DB_PATH=data/analytics.sqlite3`
- `PERSIST_RAW_SCANNER_EVENTS=false`
- `PERSIST_GATE3_SEQUENCES=false`
- `PERSIST_SCORING_BREAKDOWNS=false`
- `PERSIST_LABEL_SUGGESTIONS=false`
- `PERSIST_FEED_HEALTH=false`
- `ANALYTICS_RAW_EVENT_RETENTION_SECS=3600`
- `ANALYTICS_METRICS_RETENTION_SECS=86400`
- `ANALYTICS_EXECUTION_RETENTION_SECS=604800`

Then run:

```bash
python3 /home/ubuntu/rust-scanlink/ops/split_filter_db.py --source /home/ubuntu/rust-scanlink/data/filter.sqlite3 --runtime /home/ubuntu/rust-scanlink/data/runtime.sqlite3 --analytics /home/ubuntu/rust-scanlink/data/analytics.sqlite3
sudo systemctl daemon-reload
sudo chmod 755 /home/ubuntu/rust-scanlink/ops/analytics_janitor.py
sudo chmod 755 /home/ubuntu/rust-scanlink/ops/scanner_healthcheck.sh
sudo chmod 755 /home/ubuntu/rust-scanlink/ops/split_filter_db.py
sudo systemctl enable --now rust-scanlink.service
sudo systemctl enable --now rust-scanlink-janitor.timer
sudo systemctl enable --now rust-scanlink-healthcheck.timer
```
