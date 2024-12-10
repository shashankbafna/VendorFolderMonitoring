import os
import glob
import csv
import time
from datetime import datetime, timedelta
from statistics import median
from collections import defaultdict

# Directory where daily metrics files are stored
METRICS_DIR = "/path/to/metrics/files"
# Directory to save the generated alert reports
REPORT_DIR = "/path/to/reports"
# Threshold for delay in minutes
DELAY_THRESHOLD = 10
# Timestamp format used in metrics files
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


def parse_metrics_file(file_path):
    """Parse a metrics file and return data as a list of dictionaries."""
    metrics = []
    with open(file_path, "r") as file:
        reader = csv.reader(file, delimiter="^")
        for row in reader:
            if len(row) < 9:
                continue  # Skip malformed rows
            metrics.append({
                "capture_time": row[0],
                "folder_name": row[1],
                "file_descriptions": row[8]
            })
    return metrics


def calculate_median_window(historical_data):
    """Calculate the median window for file arrivals."""
    windows = [data["time_minutes"] for data in historical_data]
    median_minutes = median(windows)
    return median_minutes - DELAY_THRESHOLD, median_minutes + DELAY_THRESHOLD


def check_missing_files(historical_data, current_data):
    """Check for missing files in the current data based on historical data."""
    missing_files = []

    for folder_name, file_patterns in historical_data.items():
        current_files = {entry["regex"] for entry in current_data.get(folder_name, [])}

        for pattern, records in file_patterns.items():
            time_window = calculate_median_window(records)
            found_in_window = any(
                time_window[0] <= data["time_minutes"] <= time_window[1] for data in records
            )

            if not found_in_window or pattern not in current_files:
                missing_files.append((folder_name, pattern))

    return missing_files


def get_current_data():
    """Fetch the current metrics for comparison."""
    current_data = defaultdict(list)
    latest_file = max(glob.glob(os.path.join(METRICS_DIR, "*.info")), key=os.path.getctime)
    metrics = parse_metrics_file(latest_file)

    for entry in metrics:
        folder_name = entry["folder_name"]
        for file_desc in entry["file_descriptions"].split("|"):
            regex, file_info = file_desc.split("#")
            time_minutes = int(datetime.fromtimestamp(int(file_info.split(",")[2])).strftime("%H%M"))
            current_data[folder_name].append({"regex": regex, "time_minutes": time_minutes})

    return current_data


def get_historical_data():
    """Fetch historical metrics for calculating median thresholds."""
    historical_data = defaultdict(lambda: defaultdict(list))
    cutoff_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    for file_path in sorted(glob.glob(os.path.join(METRICS_DIR, "*.info"))):
        file_date = os.path.basename(file_path).split(".")[2]
        if file_date < cutoff_date:
            continue

        metrics = parse_metrics_file(file_path)
        for entry in metrics:
            folder_name = entry["folder_name"]
            for file_desc in entry["file_descriptions"].split("|"):
                regex, file_info = file_desc.split("#")
                time_minutes = int(datetime.fromtimestamp(int(file_info.split(",")[2])).strftime("%H%M"))
                historical_data[folder_name][regex].append({"time_minutes": time_minutes})

    return historical_data


def write_report(missing_files):
    """Write the missing files report to a file."""
    if not missing_files:
        return None

    report_file = os.path.join(REPORT_DIR, f"missing_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(report_file, "w") as file:
        for folder, pattern in missing_files:
            file.write(f"Folder: {folder}, Missing Pattern: {pattern}\n")

    return report_file


def main():
    historical_data = get_historical_data()
    current_data = get_current_data()
    missing_files = check_missing_files(historical_data, current_data)

    report_file = write_report(missing_files)
    if report_file:
        print(f"Alert: Missing files detected. Report generated at {report_file}")
        # You can integrate an email alert system here
    else:
        print("No missing files detected.")


if __name__ == "__main__":
    main()
