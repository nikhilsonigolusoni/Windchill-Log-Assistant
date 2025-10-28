import os
import time
import glob
import logging
import threading
import json
import socket
 
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.stats import stats as stats_module
from opencensus.stats import measure as measure_module
from opencensus.stats import view as view_module
from opencensus.stats import aggregation as aggregation_module
from opencensus.ext.azure import metrics_exporter
 
# ========================
# CONFIGURATION
# ========================
LOG_FOLDER = r"C:\ptc\Windchill_13.0\Windchill\logs"
LOG_PATTERNS = {
    "MethodServer": "MethodServer-*-log4j.log",
    "BackgroundMethodServer": "BackgroundMethodServer-*-log4j.log",
    "ServerManager": "ServerManager-*-log4j.log",
    "UpgradeManager": "UpgradeManager-*-log4j.log",
}
POLL_INTERVAL = 60  # seconds
STATE_FILE = "log_state.json"
 
CONNECTION_STRING = (""
)
 
HOSTNAME = socket.gethostname()
 
# ========================
# SETUP LOGGER (Traces)
# ========================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
trace_handler = AzureLogHandler(connection_string=CONNECTION_STRING)
if not hasattr(trace_handler, "lock") or trace_handler.lock is None:
    trace_handler.lock = threading.RLock()
 
logger.addHandler(trace_handler)
 
# ========================
# SETUP METRICS (Custom Metrics)
# ========================
measures = {
    "info_count": measure_module.MeasureInt("info_count", "Number of INFO log lines", "1"),
    "warn_count": measure_module.MeasureInt("warn_count", "Number of WARN log lines", "1"),
    "error_count": measure_module.MeasureInt("error_count", "Number of ERROR log lines", "1"),
    "heartbeat": measure_module.MeasureInt("heartbeat", "Script alive heartbeat", "1"),
}
 
views = {}
for name, measure in measures.items():
    agg = aggregation_module.CountAggregation() if "count" in name else aggregation_module.LastValueAggregation()
    views[name] = view_module.View(f"{name}_view", f"{name.replace('_', ' ').title()}", [], measure, agg)
 
stats = stats_module.stats
view_manager = stats.view_manager
for v in views.values():
    view_manager.register_view(v)
 
metrics_exporter_instance = metrics_exporter.new_metrics_exporter(connection_string=CONNECTION_STRING)
view_manager.register_exporter(metrics_exporter_instance)
 
mmap = stats.stats_recorder.new_measurement_map()
 
 
def record_metric(name: str, value: int = 1):
    if name in measures:
        mmap.measure_int_put(measures[name], value)
        mmap.record()
 
 
# ========================
# STATE MANAGEMENT
# ========================
last_positions = {}
 
 
def load_state():
    global last_positions
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                last_positions = state.get("last_positions", {})
                print("[state] Loaded state from file")
        except Exception as e:
            print(f"[state] Failed to load state: {e}")
 
 
def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"last_positions": last_positions}, f)
        print("[state] Saved state to file")
    except Exception as e:
        print(f"[state] Failed to save state: {e}")
 
 
# ========================
# LOG PROCESSING
# ========================
def send_new_logs(source: str, pattern: str):
    global last_positions
 
    log_files = glob.glob(os.path.join(LOG_FOLDER, pattern))
    for log_file in log_files:
        if log_file not in last_positions:
            last_positions[log_file] = 0
 
        try:
            with open(log_file, "r") as f:
                f.seek(last_positions[log_file])
                new_lines = f.readlines()
                last_positions[log_file] = f.tell()
 
                for line in new_lines:
                    line = line.strip()
                    if not line:
                        continue
 
                    if "ERROR" in line:
                        logger.error(line, extra={"custom_dimensions": {"hostname": HOSTNAME, "source": source}})
                        record_metric("error_count")
                    elif "WARN" in line or "WARNING" in line:
                        logger.warning(line, extra={"custom_dimensions": {"hostname": HOSTNAME, "source": source}})
                        record_metric("warn_count")
                    else:
                        logger.info(line, extra={"custom_dimensions": {"hostname": HOSTNAME, "source": source}})
                        record_metric("info_count")
 
                    print(f"[{source}] {os.path.basename(log_file)}: {line}")
 
        except Exception as e:
            logger.error(f"Failed to read {log_file}: {e}")
 
 
# ========================
# MAIN LOOP
# ========================
if __name__ == "__main__":
    print("Monitoring MethodServer, BackgroundMethodServer, ServerManager, UpgradeManager logs for metrics...")
    load_state()
    try:
        while True:
            # Process all defined log sources
            for source, pattern in LOG_PATTERNS.items():
                send_new_logs(source, pattern)
 
            # Heartbeat
            record_metric("heartbeat", 1)
 
            save_state()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping script...")
        save_state()