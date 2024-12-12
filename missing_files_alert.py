#!/usr/bin/python3
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
    filename='file_check.log',
    level=getattr(logging, args.loglevel),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Path to the directory containing metrics files
METRICS_DIR = "path_to_metrics_files"
# Lookback period for historical metrics in days
LOOKBACK_DAYS = 7
# Threshold minutes delay expected
THRESHOLD_MINS = 10
# File to maintain state of file arrivals
STATE_FILE = "file_arrival_state_" + datetime.now().strftime('%Y%m%d') + ".csv"

# Helper function to parse metrics from a CSV file
# Only parse metrics up to the current hour to optimize processing
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
                continue  # Ignore metrics for times later than the current hour today
            for entry in file_description.split("|"):
                try:
                    regex, params = entry.split("#")
                    if not validate_regex(regex):
                        logging.warning(f"Invalid regex pattern detected: {regex}")
                        continue
                    if regex in state.get(folder_name, set()):
                        logging.debug(f"Skipping already successful regex: {regex}")
                        continue  # Skip already successful file patterns
                    count, median_size, median_time, earliest, latest = eval(params)
                    metrics.append({
                        "folder": folder_name,
                        "regex": regex,
                        "capture_time": capture_time,
                        "median_time": datetime.strptime(median_time, "%H:%M").time(),
                        "earliest": datetime.fromtimestamp(earliest),
                        "latest": datetime.fromtimestamp(latest)
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
    if n % 2 == 0:  # Even number of elements
        median_time = (datetime.combine(datetime.today(), times[n // 2 - 1]) +
                       timedelta(minutes=(datetime.combine(datetime.today(), times[n // 2]) - datetime.combine(datetime.today(), times[n // 2 - 1])).seconds // 120)).time()
    else:  # Odd number of elements
        median_time = times[n // 2]
    lower_bound = (datetime.combine(datetime.today(), median_time) - timedelta(minutes=THRESHOLD_MINS)).time()
    upper_bound = (datetime.combine(datetime.today(), median_time) + timedelta(minutes=THRESHOLD_MINS)).time()
    logging.debug(f"Calculated median window: {lower_bound} - {upper_bound}")
    return lower_bound, upper_bound

# Load the state of file arrivals
def load_state():
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                folder, regex, last_latest = row
                state[(folder, regex)] = datetime.fromtimestamp(float(last_latest))
    logging.debug(f"Loaded state: {state}")
    return state

# Save the state of file arrivals
def save_state(state):
    with open(STATE_FILE, 'w') as f:
        writer = csv.writer(f)
        for (folder, regex), last_latest in state.items():
            writer.writerow([folder, regex, last_latest.timestamp()])
    logging.debug("State saved successfully.")

# Real-time folder check to confirm if a file matching the regex exists
def real_time_folder_check(folder, regex):
    folder_path = os.path.join(METRICS_DIR, folder)
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
    state = load_state()
    # Read all metrics files within the lookback period
    for file_name in os.listdir(METRICS_DIR):
        logging.debug(f"Processing file: {file_name}")
        if file_name.startswith("feed.metrics.") and file_name.endswith(".info"):
            file_date = datetime.strptime(file_name.split(".")[2], "%Y%m%d")
            if file_date >= cutoff_date:
                all_metrics.extend(parse_metrics(os.path.join(METRICS_DIR, file_name), state))
    # Group metrics by folder and regex
    grouped_metrics = defaultdict(list)
    for metric in all_metrics:
        key = (metric["folder"], metric["regex"])
        grouped_metrics[key].append(metric)
    new_state = {}
    missing_files_report = []
    for (folder, regex), records in grouped_metrics.items():
        logging.debug(f"Checking folder: {folder} with regex: {regex}")
        latest_time = max(record["latest"] for record in records if record["latest"])
        logging.debug(f"Latest historical arrival time for {folder}, {regex}: {latest_time}")
        if (folder, regex) in state:
            previous_latest = state[(folder, regex)]
            if latest_time == previous_latest:
                logging.debug(f"No change in latest arrival time for {folder}, {regex}. Skipping validation.")
                new_state[(folder, regex)] = previous_latest
                continue
            else:
                logging.debug(f"Latest arrival time changed for {folder}, {regex}. Revalidating.")
        # Check today's file arrivals
        today_records = [
            record for record in records if record["capture_time"].date() == datetime.now().date()
        ]
        times = [record["earliest"].time() for record in today_records]
        if len(times) >= 5:
            median_window = calculate_median_window(times[-5:])
            if median_window:
                lower_bound, upper_bound = median_window
                logging.debug(f"Median window for {folder}, {regex}: {lower_bound} - {upper_bound}")
                within_window = any(
                    lower_bound <= record["earliest"].time() <= upper_bound
                    for record in today_records
                )
                if within_window:
                    new_state[(folder, regex)] = latest_time
                elif not real_time_folder_check(folder, regex):
                    missing_files_report.append(
                        f"Folder: {folder}, Pattern: {regex}, Expected Window: {lower_bound}-{upper_bound}"
                    )
            else:
                logging.warning(f"Not enough data for median calculation: {folder}, {regex}.")
    # Save the updated state
    save_state(new_state)
    # Generate the report file name with a timestamp
    report_file_name = f"file_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    report_file_path = os.path.join(METRICS_DIR, report_file_name)
    # Write the report to a file
    with open(report_file_path, 'w') as f:
        if missing_files_report:
            f.write("\n".join(missing_files_report))
        else:
            f.write("No missing files detected.")
    return bool(missing_files_report)

if __name__ == "__main__":
    if check_missing_files():
        logging.error("Alert: Missing files detected. Check the report.")
    else:
        logging.info("No missing files detected.")
