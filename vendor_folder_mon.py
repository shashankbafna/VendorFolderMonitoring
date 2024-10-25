import os
import time
import csv
import statistics
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

# Constants
FEED_FOLDER = "/path/to/feed_folder"
METRICS_FILE = "/path/to/metrics.csv"
RETENTION_DAYS = 15
ROLLING_DAYS = 7
MIN_DAYS_FOR_ALERTING = 15  # Minimum days required for historical data before alerts are generated

# Function to send alert email
def send_alert(message):
    msg = MIMEText(message)
    msg['Subject'] = 'Feed Monitoring Alert'
    msg['From'] = 'monitor@yourcompany.com'
    msg['To'] = 'admin@yourcompany.com'

    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)

# Function to capture feed metrics for files modified recently
def get_feed_metrics(feed_folder):
    metrics = []
    timestamp = time.time()

    for folder_name in os.listdir(feed_folder):
        folder_path = os.path.join(feed_folder, folder_name)
        if os.path.isdir(folder_path):
            file_count = 0
            total_size = 0
            file_sizes = []
            file_arrival_times = []

            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    file_mod_time = os.path.getmtime(file_path)

                    file_count += 1
                    total_size += file_size
                    file_sizes.append(file_size)
                    file_arrival_times.append(file_mod_time)

            metrics.append({
                "folder_name": folder_name,
                "file_count": file_count,
                "total_size": total_size,
                "file_sizes": file_sizes,
                "file_arrival_times": file_arrival_times,
                "timestamp": timestamp
            })
    
    return metrics

# Function to append metrics to CSV file
def append_metrics_to_csv(metrics, metrics_file):
    file_exists = os.path.isfile(metrics_file)

    with open(metrics_file, "a", newline="") as csvfile:
        fieldnames = ["timestamp", "folder_name", "file_count", "total_size", "file_sizes", "file_arrival_times"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
        
        for metric in metrics:
            writer.writerow({
                "timestamp": metric["timestamp"],
                "folder_name": metric["folder_name"],
                "file_count": metric["file_count"],
                "total_size": metric["total_size"],
                "file_sizes": metric["file_sizes"],
                "file_arrival_times": metric["file_arrival_times"]
            })

# Function to load past metrics from CSV for historical analysis
def load_past_metrics(metrics_file, days):
    past_metrics = []
    cutoff_date = datetime.now() - timedelta(days=days)

    if os.path.exists(metrics_file):
        with open(metrics_file, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row_time = datetime.fromtimestamp(float(row["timestamp"]))
                if row_time >= cutoff_date:
                    past_metrics.append({
                        "folder_name": row["folder_name"],
                        "file_count": int(row["file_count"]),
                        "total_size": int(row["total_size"]),
                        "file_sizes": eval(row["file_sizes"]),
                        "file_arrival_times": eval(row["file_arrival_times"]),
                        "timestamp": float(row["timestamp"])
                    })
    return past_metrics

# Function to calculate daily metrics once sufficient historical data is available
def calculate_daily_metrics(today_metrics):
    folder_metrics = {}
    
    for feed in today_metrics:
        folder_name = feed['folder_name']
        
        if folder_name not in folder_metrics:
            folder_metrics[folder_name] = {
                'file_sizes': [],
                'file_arrival_times': []
            }
        
        folder_metrics[folder_name]['file_sizes'].extend(feed['file_sizes'])
        folder_metrics[folder_name]['file_arrival_times'].extend(feed['file_arrival_times'])
    
    daily_metrics = {}
    for folder_name, data in folder_metrics.items():
        all_arrival_times = data['file_arrival_times']
        all_sizes = data['file_sizes']
        
        earliest_time = min(all_arrival_times) if all_arrival_times else None
        latest_time = max(all_arrival_times) if all_arrival_times else None
        size_10th_percentile = statistics.quantiles(all_sizes, n=10)[0] if all_sizes else 0
        size_90th_percentile = statistics.quantiles(all_sizes, n=10)[8] if all_sizes else 0

        daily_metrics[folder_name] = {
            'earliest_time': earliest_time,
            'latest_time': latest_time,
            'size_10th_percentile': size_10th_percentile,
            'size_90th_percentile': size_90th_percentile
        }
    
    return daily_metrics

# Monitoring function, with alerting enabled only after 15 days of metrics have been captured
def monitor_feeds():
    today_metrics = get_feed_metrics(FEED_FOLDER)
    append_metrics_to_csv(today_metrics, METRICS_FILE)

    # Load metrics for alerting after sufficient historical data is gathered
    past_metrics = load_past_metrics(METRICS_FILE, RETENTION_DAYS)

    # Check if sufficient data exists before enabling alerting
    if len(past_metrics) < MIN_DAYS_FOR_ALERTING:
        print("Not enough historical data for alerting. Gathering data...")
        return  # Exit before alerting
    
    # Calculate daily aggregates and metrics
    all_today_metrics = load_past_metrics(METRICS_FILE, 1)
    daily_metrics = calculate_daily_metrics(all_today_metrics)

    # Alerting logic based on calculated metrics
    for folder_name, metrics in daily_metrics.items():
        if metrics['earliest_time'] and metrics['latest_time']:
            now = time.time()
            if not (metrics['earliest_time'] <= now <= metrics['latest_time']):
                send_alert(f"Feed {folder_name} files arrived outside the expected time window today.")
        
        if any(size == 0 for size in [m['file_sizes'] for m in today_metrics if m['folder_name'] == folder_name]):
            send_alert(f"Feed {folder_name} has 0-byte files.")
        
        for feed in today_metrics:
            if feed["folder_name"] == folder_name:
                if any(size < metrics['size_10th_percentile'] or size > metrics['size_90th_percentile'] for size in feed['file_sizes']):
                    send_alert(f"Feed {folder_name} has files outside the expected size range.")

# Main function
if __name__ == "__main__":
    monitor_feeds()
