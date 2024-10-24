import os
import time
import json
import statistics
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

# Constants
FEED_FOLDER = "/path/to/feed_folder"  # Path where feed folders are located
METRICS_DIR = "/path/to/metrics"  # Directory to store daily metrics in JSON
RETENTION_DAYS = 15  # Retain metrics for 15 days
ROLLING_DAYS = 7  # Number of days to calculate rolling medians, time windows, etc.

# Function to send alert email
def send_alert(message):
    msg = MIMEText(message)
    msg['Subject'] = 'Feed Monitoring Alert'
    msg['From'] = 'monitor@yourcompany.com'
    msg['To'] = 'admin@yourcompany.com'

    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)

# Function to check if a file was modified today
def is_file_modified_today(file_mod_time):
    file_mod_date = datetime.fromtimestamp(file_mod_time).date()
    today = datetime.now().date()
    return file_mod_date == today

# Function to capture feed metrics for files **only from today**
def get_feed_metrics(feed_folder):
    metrics = []

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

                    # Only consider files modified today
                    if is_file_modified_today(file_mod_time):
                        # Update metrics
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
                "timestamp": time.time()
            })
    
    return metrics

# Function to save the daily metrics to a file
def save_metrics_to_file(metrics, metrics_dir, date=None):
    # Use provided date or today's date
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    metrics_file = os.path.join(metrics_dir, f"metrics_{date}.json")
    
    # Write the metrics to a JSON file
    with open(metrics_file, "w") as f:
        json.dump(metrics, f)

# Function to load the metrics for a specific day
def load_metrics_from_file(metrics_dir, date):
    metrics_file = os.path.join(metrics_dir, f"metrics_{date}.json")
    
    if os.path.exists(metrics_file):
        with open(metrics_file, "r") as f:
            return json.load(f)
    return []

# Function to load metrics from the past X days
def load_past_metrics(metrics_dir, days):
    past_metrics = []
    for i in range(days):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        daily_metrics = load_metrics_from_file(metrics_dir, date)
        past_metrics.extend(daily_metrics)
    return past_metrics

# Function to calculate median metrics over the past X days
def calculate_median_metrics(metrics):
    folder_metrics = {}
    
    for feed in metrics:
        folder_name = feed['folder_name']
        
        if folder_name not in folder_metrics:
            folder_metrics[folder_name] = {
                'file_counts': [],
                'file_sizes': [],
                'file_arrival_times': []
            }
        
        folder_metrics[folder_name]['file_counts'].append(feed['file_count'])
        folder_metrics[folder_name]['file_sizes'].extend(feed['file_sizes'])
        folder_metrics[folder_name]['file_arrival_times'].extend(feed['file_arrival_times'])
    
    # Calculate medians and percentiles for dynamic metrics
    median_metrics = {}
    for folder_name, data in folder_metrics.items():
        median_metrics[folder_name] = {
            'median_file_count': statistics.median(data['file_counts']) if data['file_counts'] else 0,
            'median_file_size': statistics.median(data['file_sizes']) if data['file_sizes'] else 0,
            '10th_percentile_size': statistics.quantiles(data['file_sizes'], n=10)[0] if data['file_sizes'] else 0,
            '90th_percentile_size': statistics.quantiles(data['file_sizes'], n=10)[8] if data['file_sizes'] else 0,
            'median_arrival_time': statistics.median(data['file_arrival_times']) if data['file_arrival_times'] else 0,
            'max_file_size': max(data['file_sizes']) if data['file_sizes'] else 0
        }
    
    return median_metrics

# Function to calculate dynamic time window based on past data
def calculate_dynamic_time_window(metrics):
    all_arrival_times = []
    for feed in metrics:
        all_arrival_times.extend(feed['file_arrival_times'])
    
    if not all_arrival_times:
        return None, None

    earliest_time = min(all_arrival_times)
    latest_time = max(all_arrival_times)

    return earliest_time, latest_time

# Function to monitor feeds, considering dynamic time window, size ranges, and folder growth
def monitor_feeds():
    # Get today's metrics
    today_metrics = get_feed_metrics(FEED_FOLDER)
    today_date = datetime.now().strftime("%Y-%m-%d")
    save_metrics_to_file(today_metrics, METRICS_DIR, today_date)

    # Load metrics for the past X days (rolling window)
    past_metrics = load_past_metrics(METRICS_DIR, ROLLING_DAYS)
    median_metrics = calculate_median_metrics(past_metrics)

    # Compare today's metrics against historical data
    for feed in today_metrics:
        folder_name = feed['folder_name']
        file_count = feed['file_count']
        file_sizes = feed['file_sizes']
        file_arrival_times = feed['file_arrival_times']
        total_size = feed['total_size']

        # Get historical metrics for this folder
        folder_median = median_metrics.get(folder_name, {})
        median_file_count = folder_median.get('median_file_count', 0)
        median_file_size = folder_median.get('median_file_size', 0)
        max_file_size = folder_median.get('max_file_size', 50 * 1024 * 1024 * 1024)  # 50 GB default
        file_size_10th_percentile = folder_median.get('10th_percentile_size', 0)
        file_size_90th_percentile = folder_median.get('90th_percentile_size', max_file_size)

        # Dynamic time window for file arrival
        earliest_time, latest_time = calculate_dynamic_time_window(past_metrics)

        # Check for 0-byte files and trigger alert
        if any(size == 0 for size in file_sizes):
            send_alert(f"Feed {folder_name} has 0-byte files.")

        # Check if today's file count is significantly lower than the median
        if file_count < median_file_count:
            send_alert(f"Feed {folder_name} has fewer files ({file_count}) than the median ({median_file_count}).")

        # Check if today's file sizes deviate from the size range (10th-90th percentile)
        if any(size < file_size_10th_percentile or size > file_size_90th_percentile for size in file_sizes):
            send_alert(f"Feed {folder_name} has files outside the expected size range ({file_size_10th_percentile / (1024 * 1024)} MB - {file_size_90th_percentile / (1024 * 1024)} MB).")

        # Check if today's files arrived outside the dynamic time window
        current_time = time.time()
        if earliest_time and latest_time and not (earliest_time <= current_time <= latest_time):
            send_alert(f"Feed {folder_name} has files arriving outside the expected time window.")

        # Check if folder size growth is as expected
        past_folder_sizes = [feed['total_size'] for feed in past_metrics if feed['folder_name'] == folder_name]
        if total_size < statistics.median(past_folder_sizes) and total_size != 0:
            send_alert(f"Folder {folder_name} is not growing as expected. Current size: {total_size / (1024 * 1024)} MB.")
        
# Main function to run feed monitoring
if __name__ == "__main__":
    monitor_feeds
