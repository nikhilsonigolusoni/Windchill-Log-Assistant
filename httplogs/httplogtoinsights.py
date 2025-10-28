import os
import time
import logging
import socket
import json
import threading
import numpy as np
from datetime import datetime
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.stats import stats as stats_module
from opencensus.stats import measure as measure_module
from opencensus.stats import view as view_module
from opencensus.stats import aggregation as aggregation_module
from opencensus.ext.azure import metrics_exporter
import re

# ========================
# CONFIGURATION
# ========================
ACCESS_LOG = r"C:\ptc\Windchill_13.0\HTTPServer\logs\access.log"
SCRIPT_LOG = "script.log"
STATE_FILE = "log_state.json"
POLL_INTERVAL = 60  # seconds

CONNECTION_STRING = ""
HOSTNAME = socket.gethostname()

# ========================
# SETUP LOGGER
# ========================
logger = logging.getLogger("access_log_monitor")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
file_handler = logging.FileHandler(SCRIPT_LOG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

trace_handler = AzureLogHandler(connection_string=CONNECTION_STRING)
if not hasattr(trace_handler, "lock") or trace_handler.lock is None:
    trace_handler.lock = threading.RLock()
logger.addHandler(trace_handler)

# ========================
# SETUP METRICS
# ========================
measures = {
    "response_time": measure_module.MeasureInt("response_time", "HTTP Response Time in ms", "ms"),
    "avg_response_time": measure_module.MeasureInt("avg_response_time", "Average HTTP Response Time per interval", "ms"),
    "error_http_count": measure_module.MeasureInt("error_http_count", "Number of HTTP Errors (>=400)", "1"),
    "request_count": measure_module.MeasureInt("request_count", "Requests per interval", "1"),
    "error_rate": measure_module.MeasureFloat("error_rate", "HTTP Error %", "%"),
    "heartbeat": measure_module.MeasureInt("heartbeat", "Script alive heartbeat", "1")
}

views = {}
# views["response_time"] = view_module.View(
#     "response_time_view",
#     "Response Time Distribution",
#     [],
#     measures["response_time"],
#     aggregation_module.DistributionAggregation([0,50,100,200,500,1000,2000,5000,10000])
# )

views["response_time"] = view_module.View(
    "response_time_view",
    "Response Time",
    [],
    measures["response_time"],
    aggregation_module.LastValueAggregation()
)

views["avg_response_time"] = view_module.View(
    "avg_response_time_view",
    "Average Response Time per interval",
    [],
    measures["avg_response_time"],
    aggregation_module.LastValueAggregation()
)
views["error_http_count"] = view_module.View(
    "error_http_count_view",
    "HTTP Errors Count",
    [],
    measures["error_http_count"],
    aggregation_module.CountAggregation()
)
views["request_count"] = view_module.View(
    "request_count_view",
    "Requests Count",
    [],
    measures["request_count"],
    aggregation_module.CountAggregation()
)
views["error_rate"] = view_module.View(
    "error_rate_view",
    "Error Rate",
    [],
    measures["error_rate"],
    aggregation_module.LastValueAggregation()
)
views["heartbeat"] = view_module.View(
    "heartbeat_view",
    "Heartbeat",
    [],
    measures["heartbeat"],
    aggregation_module.LastValueAggregation()
)

# Register views
stats = stats_module.stats
view_manager = stats.view_manager
for v in views.values():
    view_manager.register_view(v)

metrics_exporter_instance = metrics_exporter.new_metrics_exporter(connection_string=CONNECTION_STRING)
view_manager.register_exporter(metrics_exporter_instance)
mmap = stats.stats_recorder.new_measurement_map()

def record_metric(name, value):
    if name in measures:
        if isinstance(value,int):
            mmap.measure_int_put(measures[name], value)
        else:
            mmap.measure_float_put(measures[name], value)
        mmap.record()

# ========================
# STATE MANAGEMENT
# ========================
last_access_pos = 0
def load_state():
    global last_access_pos
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE,"r") as f:
                state = json.load(f)
                last_access_pos = state.get("last_access_pos",0)
                logger.info("[state] Loaded state from file")
        except Exception as e:
            logger.error(f"[state] Failed to load state: {e}")

def save_state():
    try:
        with open(STATE_FILE,"w") as f:
            json.dump({"last_access_pos": last_access_pos}, f)
        logger.info("[state] Saved state to file")
    except Exception as e:
        logger.error(f"[state] Failed to save state: {e}")

# ========================
# PARSE ACCESS LOG LINE
# ========================
log_pattern = re.compile(r'(\S+) - (\S+) \[(.*?)\] "(.*?) (.*?) (HTTP/\d\.\d)" (\d{3}) (\d+) (\d+)')

def parse_log_line(line):
    match = log_pattern.match(line)
    if match:
        client_ip = match.group(1)
        user = match.group(2)
        timestamp = match.group(3)
        method = match.group(4)
        url = match.group(5)
        protocol = match.group(6)
        status = int(match.group(7))
        size = int(match.group(8))
        response_time = int(match.group(9))
        return {
            "client_ip": client_ip,
            "user": user,
            "timestamp": timestamp,
            "method": method,
            "url": url,
            "protocol": protocol,
            "status": status,
            "size": size,
            "response_time": response_time
        }
    return None

# ========================
# SEND ACCESS LOG METRICS & TRACES
# ========================
def send_access_metrics():
    global last_access_pos

    if not os.path.exists(ACCESS_LOG):
        logger.warning(f"{ACCESS_LOG} does not exist")
        return

    request_count = 0
    error_count = 0
    response_times = []

    with open(ACCESS_LOG,"r") as f:
        f.seek(last_access_pos)
        lines = f.readlines()
        last_access_pos = f.tell()

        for line in lines:
            line = line.strip()
            if not line: continue
            log_data = parse_log_line(line)
            if not log_data: continue

            request_count += 1
            response_times.append(log_data["response_time"])
            record_metric("response_time", log_data["response_time"])
            if log_data["status"] >= 400:
                error_count += 1
                record_metric("error_http_count",1)

            # Send structured log to App Insights
            logger.info(
                f"{log_data['method']} {log_data['url']} {log_data['status']} {log_data['response_time']}ms",
                extra={"custom_dimensions": {**log_data, "hostname": HOSTNAME, "source":"AccessLog"}}
            )

    if request_count > 0:
        record_metric("request_count", request_count)
        error_rate = (error_count/request_count)*100
        record_metric("error_rate", error_rate)
        avg_response = int(np.mean(response_times))
        record_metric("avg_response_time", avg_response)

        logger.info(
            f"[Metrics] Requests:{request_count}, Errors:{error_count}, ErrorRate:{error_rate:.2f}%, "
            f"Avg:{avg_response}ms, "
            f"p50:{int(np.percentile(response_times,50))}ms, "
            f"p90:{int(np.percentile(response_times,90))}ms, "
            f"p99:{int(np.percentile(response_times,99))}ms"
        )

# ========================
# MAIN LOOP
# ========================
if __name__=="__main__":
    logger.info("Starting access.log monitoring script...")
    load_state()
    try:
        while True:
            send_access_metrics()
            record_metric("heartbeat",1)
            save_state()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping script...")
        save_state()
