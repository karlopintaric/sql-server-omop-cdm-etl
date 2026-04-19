#!/bin/bash
set -e
# Start/restart Dagster services

echo "Reloading systemd and restarting Dagster services..."
sudo systemctl daemon-reload
sudo systemctl restart dagster-webserver dagster-daemon

echo ""
echo "Services restarted. Status:"
sudo systemctl status dagster-webserver --no-pager -l
sudo systemctl status dagster-daemon --no-pager -l

echo ""
echo "View logs with:"
echo "  sudo journalctl -u dagster-webserver -f"
echo "  sudo journalctl -u dagster-daemon -f"
