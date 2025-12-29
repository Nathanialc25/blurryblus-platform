#!/bin/bash
# start_airflow_all.sh
# Clean restart for Airflow 2.9.2

AIRFLOW_HOME="$HOME/airflow"
DAGS_PATH="$AIRFLOW_HOME/dags"
LOGS_PATH="$AIRFLOW_HOME/logs"

# Load environment variables
if [ -f "$HOME/airflow/.env" ]; then
    export $(grep -v '^#' "$HOME/airflow/.env" | xargs)
fi

# Ensuring Logs path is real
mkdir -p "$LOGS_PATH"

# Grabbing this scripts ID
echo "[INFO] Performing complete clean shutdown..."
SCRIPT_PID=$$
echo "[INFO] Startup script PID: $SCRIPT_PID"

# Extracting the External IP address for web use
EXTERNAL_IP=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip)

# Kill specific Airflow Processes (suppress error message, and return "the not found")
for proc in "airflow webserver" "airflow scheduler" "airflow triggerer" "cloud-sql-proxy" "airflow celery worker" "celery"; do
    pkill -f "$proc" 2>/dev/null || echo "[INFO] No $proc found"
done

sleep 5

# Force kill lingering Processes wiht -9 (suppress the error message, ensures the script continues regardless)
for proc in "airflow webserver" "airflow scheduler" "airflow triggerer" "cloud-sql-proxy" "airflow celery worker" "celery"; do
    pkill -9 -f "$proc" 2>/dev/null || true
done

# Looks through processes, zombie ones, related to airflow, brings back PID from second row, and kill those PIDs.
ps -ef | grep defunct | grep airflow | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true

sleep 3

# --- Start Cloud SQL Proxy ---
echo "[INFO] Starting Cloud SQL Proxy..."
nohup cloud-sql-proxy blurryblus:us-central1:blurryblus-db \
    --credentials-file="$AIRFLOW_HOME/airflow-sql-proxy-key.json" \
    --port=5432 > "$LOGS_PATH/cloudsql-proxy.log" 2>&1 &

echo "[INFO] Waiting for Cloud SQL Proxy..."
for i in {1..15}; do
    if nc -z localhost 5432 2>/dev/null; then  #is this port taking connections??
        echo "[INFO] Cloud SQL Proxy ready!"
        break
    fi
    echo "[INFO] Waiting for Cloud SQL Proxy... ($i/15)"
    sleep 2
done

# Check Redis
if ! nc -z localhost 6379 2>/dev/null; then #is this port taking connections??
    echo "[ERROR] Redis is not running on port 6379"
    exit 1
else
    echo "[INFO] Redis is ready!"
fi

# Set PYTHONPATH (Where to look for modules when importing)
export PYTHONPATH="$DAGS_PATH:$PYTHONPATH"
echo "[INFO] PYTHONPATH set to: $PYTHONPATH"

# --- Start Airflow Webserver ---
echo "[INFO] Starting Airflow webserver..."
nohup airflow webserver --host 0.0.0.0 --port 8081 > "$LOGS_PATH/webserver.log" 2>&1 &

echo "[INFO] Waiting for webserver..."
for i in {1..30}; do
    if curl -s http://127.0.0.1:8081/health | grep -q "healthy"; then
        echo "[INFO] Webserver health check PASSED"
        break
    fi
    echo "[INFO] Waiting for webserver... ($i/30)"
    sleep 2
done

# --- Start Scheduler ---
echo "[INFO] Starting Airflow scheduler..."
nohup airflow scheduler > "$LOGS_PATH/scheduler.log" 2>&1 &

# --- Start Triggerer ---
echo "[INFO] Starting triggerer..."
nohup airflow triggerer > "$LOGS_PATH/triggerer.log" 2>&1 &

# --- Start Celery worker ---
echo "[INFO] Starting Celery worker..."
nohup airflow celery worker > "$LOGS_PATH/celery-worker.log" 2>&1 &

# Wait a moment for all components
sleep 10

# Final status check
echo "=== Final Status Check ==="
ps aux | grep -E "(airflow|cloud-sql-proxy)" | grep -v grep | grep -v "$SCRIPT_PID"

echo "[INFO] Webserver Health:"
curl -s http://127.0.0.1:8081/health || echo "Webserver health check failed"

echo "[INFO] Startup complete!"
echo "[INFO] Logs: $LOGS_PATH"
echo "[INFO] Webserver URL: http://localhost:8081"
echo "[INFO] External IP: $EXTERNAL_IP"