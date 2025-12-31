#!/bin/bash
# Flight Price Fetcher Cron Script
# Runs daily at 10 AM via cron
#
# Cron setup: crontab -e
# Add: 0 10 * * * /path/to/MYOL-neer/run_flight_fetcher.sh

cd "$(dirname "$0")"

mkdir -p logs
LOG_FILE="logs/docker_$(date +%Y%m%d_%H%M%S).log"

docker compose up --build 2>&1 | tee "$LOG_FILE"
