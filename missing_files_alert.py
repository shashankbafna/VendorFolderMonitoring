!/usr/bin/python3
import os
import csv
import re
import logging
import statistics
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

# Argument parser for setting log level dynamically
parser = argparse.ArgumentParser(description="File Monitoring Script")
parser.add_argument(
    "--loglevel", 
    default="INFO", 
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    help="Set the logging level (default: INFO)"
)
args = parser.parse_args()

# Configure logging
logging.basicConfig(
    filename=f'file_check_{datetime.now().strftime('%Y%m%d')}.log',
    level=getattr(logging, args.loglevel),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Paths
FEED_DIR = "path_to_feed_directory"
METRICS_DIR = "path_to_metrics_files"
LOOKBACK_DAYS = 7
THRESHOLD_MINS = 10
# Report file:
report_file_name = f"file_report_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
# State file paths
STATE_FILE_SUCCESS = f"file_arrival_success_state_{datetime.now().strftime('%Y%m%d')}.csv"
STATE_FILE_FAILURE = f"file_arrival_failure_state_{datetime.now().strftime('%Y%m%d')}.csv"

# Helper function to parse metrics from a CSV file
def parse_metrics(file_path, state):
    metrics = []
    current_time = datetime.now()
    current_hour = current_time.hour
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter='^')
        next(reader)  # Skip the header
        for row in reader:
            file_description = row[8]
            folder_name = row[1]
            capture_time = datetime.strptime(row[0], "%Y%m%d_%H%M%S")
            logging.debug(f"Parsing row: {row}")
            if capture_time.hour > current_hour or file_description == "None":
                logging.debug("Skipping record: Capture time exceeds current hour or file description is None.")
                continue
            for entry in file_description.split("|"):
                try:
                    regex, params = entry.split("#")
                    params = params.strip()
                    if not validate_regex(regex):
                        logging.warning(f"Invalid regex pattern detected: {regex}")
                        continue
                    count, median_size, median_time, earliest, latest = params.split(',')
                    if count != "None" and median_size != "None" and median_time != "None" and earliest != "None" and latest != "None":
                        metrics.append({
                            "folder": folder_name,
                            "regex": regex,
                            "capture_time": capture_time,
                            "median_time": datetime.strptime(median_time, "%H:%M").time(),
                            "earliest": datetime.fromtimestamp(int(earliest)),
                            "latest": datetime.fromtimestamp(int(latest))
                        })
                except Exception as e:
                    logging.error(f"Error parsing entry: {entry} in file {file_path} - {e}")
    return metrics

# Helper function to validate regex patterns
def validate_regex(pattern):
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False

# Helper function to calculate median time range with a threshold
def calculate_median_window(times):
    if not times:
        logging.debug("No times available to calculate median window.")
        return None
    times.sort()
    n = len(times)
    if n % 2 == 0:
        median_time = (datetime.combine(datetime.today(), times[n // 2 - 1]) +
                       timedelta(minutes=(datetime.combine(datetime.today(), times[n // 2]) - datetime.combine(datetime.today(), times[n // 2 - 1])).seconds // 120)).time()
    else:
        median_time = times[n // 2]
    lower_bound = (datetime.combine(datetime.today(), median_time) - timedelta(minutes=THRESHOLD_MINS)).time()
    upper_bound = (datetime.combine(datetime.today(), median_time) + timedelta(minutes=THRESHOLD_MINS)).time()
    logging.debug(f"Calculated median window: {lower_bound} - {upper_bound}")
    return lower_bound, upper_bound

# Load the state of file arrivals
def load_state(state_file):
    state = {}
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                folder, regex, count, last_time, status = row
                state[(folder, regex)] = {
                    "count": int(count),
                    "last_time": datetime.strptime(last_time, "%Y%m%d-%H%M%S"),
                    "status": status == "True"
                }
    logging.debug(f"Loaded state from {state_file}: {state}")
    return state

# Save the state of file arrivals
def save_state(state, state_file):
    with open(state_file, 'w') as f:
        writer = csv.writer(f)
        for (folder, regex), data in state.items():
            writer.writerow([folder, regex, data["count"], data["last_time"].strftime("%Y%m%d-%H%M%S"), data["status"]])
    logging.debug(f"State saved to {state_file}.")

# Real-time folder check to confirm if a file matching the regex exists
def real_time_folder_check(folder, regex):
    folder_path = os.path.join(FEED_DIR, folder)
    logging.debug(f"Checking folder: {folder_path} for regex: {regex}")
    if not os.path.exists(folder_path):
        logging.info(f"Folder not found: {folder_path}")
        return False
    try:
        pattern = re.compile(regex)
        today = datetime.now().date()
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path) and datetime.fromtimestamp(os.path.getmtime(file_path)).date() == today:
                if pattern.match(file_name):
                    logging.info(f"File matching regex '{regex}' found in folder '{folder}'.")
                    return True
        logging.info(f"No files matching regex '{regex}' found in folder '{folder}' for today.")
    except Exception as e:
        logging.error(f"Error checking files in folder {folder} with regex {regex}: {e}")
    return False

# Main function to check for missing files
def check_missing_files():
    all_metrics = []
    cutoff_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    success_state = load_state(STATE_FILE_SUCCESS)
    failure_state = load_state(STATE_FILE_FAILURE)

    # Read all metrics files within the lookback period
    for file_name in os.listdir(METRICS_DIR):
        logging.debug(f"Processing file: {file_name}")
        if file_name.startswith("feed.metrics.") and file_name.endswith(".info"):
            file_date = datetime.strptime(file_name.split(".")[2], "%Y%m%d")
            if file_date >= cutoff_date:
                all_metrics.extend(parse_metrics(os.path.join(METRICS_DIR, file_name), success_state))

    # Group metrics by folder and regex
    grouped_metrics = defaultdict(list)
    for metric in all_metrics:
        key = (metric["folder"], metric["regex"])
        grouped_metrics[key].append(metric)

    new_success_state = {}
    new_failure_state = {}
    missing_files_report = []

    for (folder, regex), records in grouped_metrics.items():
        logging.debug(f"Checking folder: {folder} with regex: {regex}")
        latest_time = max(record["latest"] for record in records if record["latest"])
        logging.debug(f"Latest historical arrival time for {folder}, {regex}: {latest_time}")

        if (folder, regex) in success_state:
            previous_latest = success_state[(folder, regex)]["last_time"]
            if latest_time == previous_latest:
                logging.debug(f"No change in latest arrival time for {folder}, {regex}. Skipping validation.")
                new_success_state[(folder, regex)] = success_state[(folder, regex)]
                continue
            else:
                logging.debug(f"Latest arrival time changed for {folder}, {regex}. Revalidating.")

        # Filter only historical records for median calculation
        historical_records = [
            record for record in records if record["capture_time"].date() < datetime.now().date()
        ]
        times = [record["earliest"].time() for record in historical_records]

        if len(times) >= 5:
            median_window = calculate_median_window(times[-5:])
            if median_window:
                lower_bound, upper_bound = median_window
                logging.debug(f"Median window for {folder}, {regex}: {lower_bound} - {upper_bound}")

                # Check today's file arrivals against the median window
                today_records = [
                    record for record in records if record["capture_time"].date() == datetime.now().date()
                ]
                within_window = any(
                    lower_bound <= record["earliest"].time() <= upper_bound
                    for record in today_records
                )
                if within_window:
                    new_success_state[(folder, regex)] = {
                        "count": len(today_records),
                        "last_time": latest_time,
                        "status": True
                    }
                elif not real_time_folder_check(folder, regex):
                    missing_files_report.append(
                        f"Folder: {folder}, Pattern: {regex}, Expected Window: {lower_bound}-{upper_bound}"
                    )
                    new_failure_state[(folder, regex)] = {
                        "count": 0,
                        "last_time": latest_time,
                        "status": False
                    }
            else:
                logging.warning(f"Not enough data for median calculation: {folder}, {regex}.")
        else:
            logging.warning(f"Insufficient historical records for {folder}, {regex}.")

    # Save the updated state
    save_state(new_success_state, STATE_FILE_SUCCESS)
    save_state(new_failure_state, STATE_FILE_FAILURE)

    # Generate and write the report
    with open(report_file_name, "w") as report_file:
        for entry in missing_files_report:
            report_file.write(entry + "\n")
    logging.info(f"Missing files report generated: {report_file_name}")

if __name__ == "__main__":
    check_missing_files()
