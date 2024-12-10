import os
import csv
import time
from datetime import datetime, timedelta
from statistics import median
from collections import defaultdict

# Configuration
METRICS_DIR = "/path/to/metrics/files"  # Directory containing metrics files
REPORT_FILE = "/path/to/alert_report.txt"  # Output report file
ALERT_EMAIL = "admin@example.com"  # Email for alerts
THRESHOLD_MINUTES = 10  # Threshold for expected file arrival in minutes
DAYS_LOOKBACK = 5  # Lookback period for median calculation

# Helper functions
def parse_metrics_file(file_path):
    """Parses a single metrics file and returns a dictionary of folder data."""
    folder_data = defaultdict(list)
    with open(file_path, "r") as f:
        reader = csv.reader(f, delimiter="^")
        for row in reader:
            if len(row) < 9:
                continue
            capture_time = datetime.strptime(row[0], "%Y%m%d_%H%M%S")
            folder_name = row[1]
            file_description = row[8]
            for file_entry in file_description.split("|"):
                try:
                    regex, metrics = file_entry.split("#")
                    metrics = metrics.strip("()")
                    size, mod_time, earliest, latest = metrics.split(",")
                    folder_data[(folder_name, regex)].append({
                        "capture_time": capture_time,
                        "size": int(size),
                        "mod_time": datetime.strptime(mod_time, "%H:%M"),
                        "earliest": int(earliest),
                        "latest": int(latest),
                    })
                except ValueError:
                    continue
    return folder_data

def load_recent_metrics():
    """Loads metrics from the past DAYS_LOOKBACK days."""
    recent_data = defaultdict(list)
    cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
    for file_name in os.listdir(METRICS_DIR):
        if not file_name.startswith("feed-metrics_") or not file_name.endswith(".info"):
            continue
        date_str = file_name.split("_")[1].split(".")[0]
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue
        if file_date >= cutoff_date:
            file_path = os.path.join(METRICS_DIR, file_name)
            file_data = parse_metrics_file(file_path)
            for key, records in file_data.items():
                recent_data[key].extend(records)
    return recent_data

def calculate_median_window(records):
    """Calculates the median modified time and returns a time window."""
    mod_times = [record["mod_time"] for record in records]
    if not mod_times:
        return None, None
    median_time = median(mod_times)
    start_time = (datetime.combine(datetime.today(), median_time) - timedelta(minutes=THRESHOLD_MINUTES)).time()
    end_time = (datetime.combine(datetime.today(), median_time) + timedelta(minutes=THRESHOLD_MINUTES)).time()
    return start_time, end_time

def check_for_missing_files(recent_data):
    """Checks for missing files based on expected arrival times."""
    missing_files = []
    for (folder_name, regex), records in recent_data.items():
        if len(records) < DAYS_LOOKBACK:
            # Skip if not enough historical data
            continue
        start_time, end_time = calculate_median_window(records)
        if not start_time or not end_time:
            continue
        # Check today's records
        today = datetime.now().date()
        today_records = [
            record for record in records
            if record["capture_time"].date() == today
        ]
        found = any(
            start_time <= record["capture_time"].time() <= end_time
            for record in today_records
        )
        if not found:
            missing_files.append({
                "folder": folder_name,
                "regex": regex,
                "expected_window": f"{start_time} - {end_time}",
            })
    return missing_files

def generate_report(missing_files):
    """Generates a report of missing files."""
    with open(REPORT_FILE, "w") as f:
        if not missing_files:
            f.write("No missing files detected.\n")
            return
        f.write("Missing Files Report\n")
        f.write("===================\n")
        for entry in missing_files:
            f.write(
                f"Folder: {entry['folder']}, Pattern: {entry['regex']}, "
                f"Expected Window: {entry['expected_window']}\n"
            )

def send_alert_email():
    """Sends an alert email if the report is not empty."""
    with open(REPORT_FILE, "r") as f:
        content = f.read()
        if "No missing files detected" in content:
            return
        os.system(f"mail -s 'Missing Files Alert' {ALERT_EMAIL} < {REPORT_FILE}")

# Main script logic
def main():
    recent_data = load_recent_metrics()
    missing_files = check_for_missing_files(recent_data)
    generate_report(missing_files)
    send_alert_email()

if __name__ == "__main__":
    main()
