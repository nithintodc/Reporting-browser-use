#!/bin/bash
# =============================================================================
# Step 3: Scheduling — Run this INSIDE the GCE VM
# Sets up cron job + optional VM auto-start/stop to save costs
# =============================================================================
set -euo pipefail

APP_DIR="/opt/doordash-bot"

echo "============================================"
echo " Scheduling Setup"
echo "============================================"
echo ""
echo "Choose a scheduling mode:"
echo "  1) Cron on always-on VM (~\$97/month compute)"
echo "  2) Cron on VM + auto-shutdown after run (~\$16-25/month compute)"
echo "     (requires Cloud Scheduler to start the VM before each run)"
echo ""
read -p "Enter 1 or 2: " MODE

# ──────────────────────────────────────────────────────────────────────────────
# Common: Create the run wrapper script
# ──────────────────────────────────────────────────────────────────────────────
cat > "$APP_DIR/run_scheduled.sh" << 'RUNSCRIPT'
#!/bin/bash
# Scheduled run wrapper — ensures Chrome is up, runs the app, optionally shuts down
set -e

APP_DIR="/opt/doordash-bot"
LOG_FILE="$APP_DIR/logs/cron_$(date +%Y%m%d_%H%M%S).log"

cd "$APP_DIR"
source .venv/bin/activate

# Ensure Xvfb + Chrome are running
sudo systemctl start xvfb 2>/dev/null || true
sleep 1
sudo systemctl start chrome-headless 2>/dev/null || true
sleep 3

# Verify Chrome CDP
for i in $(seq 1 20); do
    if curl -s http://localhost:9222/json/version > /dev/null 2>&1; then
        break
    fi
    echo "[run_scheduled] Waiting for Chrome CDP... ($i/20)"
    sleep 2
done

echo "[run_scheduled] Starting at $(date)" | tee -a "$LOG_FILE"
python main.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
echo "[run_scheduled] Finished at $(date) with exit code $EXIT_CODE" | tee -a "$LOG_FILE"

# If AUTO_SHUTDOWN is set (mode 2), shut down after run
if [ "${AUTO_SHUTDOWN:-0}" = "1" ]; then
    echo "[run_scheduled] Auto-shutdown enabled — shutting down VM in 60s..."
    sleep 60
    sudo shutdown -h now
fi

exit $EXIT_CODE
RUNSCRIPT

chmod +x "$APP_DIR/run_scheduled.sh"

if [ "$MODE" = "1" ]; then
    # ──────────────────────────────────────────────────────────────────────────
    # Mode 1: Simple cron (VM stays on 24/7)
    # ──────────────────────────────────────────────────────────────────────────
    echo ""
    read -p "What time to run daily? (UTC, 24h format, e.g. 06:00): " RUN_TIME
    HOUR=$(echo "$RUN_TIME" | cut -d: -f1)
    MIN=$(echo "$RUN_TIME" | cut -d: -f2)

    # Add cron job
    CRON_LINE="$MIN $HOUR * * * $APP_DIR/run_scheduled.sh"

    (crontab -l 2>/dev/null | grep -v "run_scheduled.sh"; echo "$CRON_LINE") | crontab -

    echo ""
    echo "    Cron job added: $CRON_LINE"
    echo "    Verify with: crontab -l"
    echo ""
    echo "    Logs will be at: $APP_DIR/logs/cron_*.log"

elif [ "$MODE" = "2" ]; then
    # ──────────────────────────────────────────────────────────────────────────
    # Mode 2: Run on boot + auto-shutdown (VM started by Cloud Scheduler)
    # ──────────────────────────────────────────────────────────────────────────

    # Set AUTO_SHUTDOWN so the wrapper shuts down after run
    if ! grep -q "AUTO_SHUTDOWN" "$APP_DIR/.env" 2>/dev/null; then
        echo "AUTO_SHUTDOWN=1" >> "$APP_DIR/.env"
    fi

    # Create systemd service that runs on boot
    sudo tee /etc/systemd/system/doordash-bot.service > /dev/null << 'BOTSERVICE'
[Unit]
Description=DoorDash Automation Bot (runs on boot, shuts down when done)
After=chrome-headless.service network-online.target
Requires=chrome-headless.service
Wants=network-online.target

[Service]
Type=oneshot
Environment=AUTO_SHUTDOWN=1
WorkingDirectory=/opt/doordash-bot
ExecStart=/opt/doordash-bot/run_scheduled.sh
StandardOutput=journal
StandardError=journal
TimeoutStartSec=7200
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
BOTSERVICE

    sudo systemctl daemon-reload
    sudo systemctl enable doordash-bot.service

    echo ""
    echo "    Boot-trigger service created and enabled."
    echo ""
    echo "    Now set up Cloud Scheduler on your laptop to start the VM daily:"
    echo ""
    echo "    # From your laptop, run:"
    echo "    gcloud scheduler jobs create http doordash-daily-start \\"
    echo "        --schedule='0 12 * * *' \\"
    echo "        --uri='https://compute.googleapis.com/compute/v1/projects/\$PROJECT_ID/zones/us-central1-a/instances/doordash-bot/start' \\"
    echo "        --http-method=POST \\"
    echo "        --oauth-service-account-email=doordash-automation@\$PROJECT_ID.iam.gserviceaccount.com \\"
    echo "        --time-zone='America/Chicago' \\"
    echo "        --location=us-central1"
    echo ""
    echo "    The VM will:"
    echo "      1. Start (triggered by Cloud Scheduler)"
    echo "      2. Run the automation (via doordash-bot.service)"
    echo "      3. Shut itself down when complete"
    echo "      4. You only pay for compute time used (~2-4 hrs/day)"
fi

echo ""
echo "============================================"
echo " Scheduling configured!"
echo "============================================"
