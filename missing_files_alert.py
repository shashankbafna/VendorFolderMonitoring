import os
import csv
import statistics
from datetime import datetime, timedelta
from collections import defaultdict

# Path to the directory containing metrics files
METRICS_DIR = os.getenv("path_to_metrics_files")
# Path to the output report file
REPORT_FILE = os.getenv("path_to_metrics_files")+"/missing_files_report.txt"
# Lookback period for historical metrics in days
LOOKBACK_DAYS = 7
# Threshold for late file arrivals
THRESHOLD_MINUTES = 10

# Helper function to parse metrics from a CSV file
def parse_metrics(file_path):
    metrics = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter='^')
        next(reader)  # Skip the header
        for row in reader:
            file_description = row[8]
            folder_name = row[1]
            capture_time = datetime.strptime(row[0], "%Y%m%d_%H%M%S")
            if file_description != "None":
                for entry in file_description.split("|"):
                    regex, params = entry.split("#")
                    count, median_size, median_time, earliest, latest = eval(params)
                    metrics.append({
                        "folder": folder_name,
                        "regex": regex,
                        "capture_time": capture_time,
                        "median_time": datetime.strptime(median_time, "%H:%M").time(),
                        "earliest": datetime.fromtimestamp(earliest),
                        "latest": datetime.fromtimestamp(latest)
                    })
    return metrics

# Helper function to calculate median time range with a threshold
def calculate_median_window(times):
    if not times:
        return None
    times.sort()
    n = len(times)
    if n % 2 == 0:  # Even number of elements
        median_time = (datetime.combine(datetime.today(), times[n // 2 - 1]) + datetime.combine(datetime.today(), times[n // 2])).time()
    else:  # Odd number of elements
        median_time = times[n // 2]
    lower_bound = (datetime.combine(datetime.today(), median_time) - timedelta(minutes=THRESHOLD_MINUTES)).time()
    upper_bound = (datetime.combine(datetime.today(), median_time) + timedelta(minutes=THRESHOLD_MINUTES)).time()
    return lower_bound, upper_bound

# Main function to check for missing files
def check_missing_files():
    all_metrics = []
    cutoff_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    # Read all metrics files within the lookback period
    for file_name in os.listdir(METRICS_DIR):
        if file_name.endswith(".info"):
            file_date = datetime.strptime(file_name.split(".")[2], "%Y%m%d")
            if file_date >= cutoff_date:
                all_metrics.extend(parse_metrics(os.path.join(METRICS_DIR, file_name)))
    # Group metrics by folder and regex
    grouped_metrics = defaultdict(list)
    for metric in all_metrics:
        key = (metric["folder"], metric["regex"])
        grouped_metrics[key].append(metric)
    # Check for missing files
    missing_files_report = []
    for (folder, regex), records in grouped_metrics.items():
        valid_records = [record for record in records if record["earliest"] and record["latest"]]
        times = [record["earliest"].time() for record in valid_records]
        if len(times) >= 5:
            median_window = calculate_median_window(times[-5:])
            if median_window:
                lower_bound, upper_bound = median_window
                # Check if today's file is within the window
                today_records = [
                    record for record in valid_records
                    if record["capture_time"].date() == datetime.today().date()
                ]
                within_window = any(
                    lower_bound <= record["earliest"].time() <= upper_bound
                    for record in today_records
                )
                if not within_window:
                    missing_files_report.append(
                        f"Folder: {folder}, Pattern: {regex}, Expected Window: {lower_bound}-{upper_bound}"
                    )
    # Write the report to a file
    with open(REPORT_FILE, 'w') as f:
        if missing_files_report:
            f.write("\n".join(missing_files_report))
        else:
            f.write("No missing files detected.")
    return bool(missing_files_report)

if __name__ == "__main__":
    if check_missing_files():
        print("Alert: Missing files detected. Check the report.")
    else:
        print("No missing files detected.")
