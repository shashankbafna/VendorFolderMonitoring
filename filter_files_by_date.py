import os
import re
import argparse
from datetime import datetime

def extract_date_from_filename(filename):
    date_patterns = [
        r'\b(\d{4})(\d{2})(\d{2})\b',       # YYYYMMDD
        r'\b(\d{4})[_.](\d{2})[_.](\d{2})\b', # YYYY.MM.DD or YYYY_MM_DD
        r'\b(\d{2})(\d{2})(\d{4})\b',       # DDMMYYYY
        r'\b(\d{2})(\d{4})(\d{2})\b',       # MMYYYYDD
        r'\b(\d{8})\b'                        # Ambiguous: 20241212 or 12122024
    ]
    for pattern in date_patterns:
        match = re.search(pattern, filename)
        if match:
            return match.groups()
    return None

def parse_ambiguous_date(date_string):
    """Handle ambiguous cases like 20241212 or 12122024."""
    try:
        # Attempt YYYYMMDD first
        return datetime.strptime(date_string, "%Y%m%d").date()
    except ValueError:
        try:
            # Attempt DDMMYYYY
            return datetime.strptime(date_string, "%d%m%Y").date()
        except ValueError:
            try:
                # Attempt MMDDYYYY
                return datetime.strptime(date_string, "%m%d%Y").date()
            except ValueError:
                return None

def approx_date_by_mtime(file_path):
    mtime = os.path.getmtime(file_path)
    return datetime.fromtimestamp(mtime).date()

def main(folder):
    today = datetime.now().date()
    matching_files = []
    error_files = []

    for root, _, files in os.walk(folder):
        for file in files:
            file_path = os.path.join(root, file)
            date_parts = extract_date_from_filename(file)

            if date_parts:
                try:
                    if len(date_parts) == 1:  # Ambiguous full date string like 20241212
                        date = parse_ambiguous_date(date_parts[0])
                    elif len(date_parts[0]) == 4:  # Likely YYYY based
                        date = datetime.strptime(''.join(date_parts), "%Y%m%d").date()
                    elif len(date_parts[1]) == 4:  # Likely MMYYYYDD or DDYYYYMM
                        # Attempt MMYYYYDD first
                        try:
                            date = datetime.strptime(''.join(date_parts), "%m%Y%d").date()
                        except ValueError:
                            # Fallback to DDYYYYMM
                            date = datetime.strptime(''.join(date_parts), "%d%Y%m").date()
                    else:  # Likely DDMMYYYY or MMDDYYYY
                        date = datetime.strptime(''.join(date_parts), "%d%m%Y").date()
                except (ValueError, TypeError):
                    # Ambiguous date, use mtime
                    date = approx_date_by_mtime(file_path)

                if date == today:
                    matching_files.append(file_path)
            else:
                error_files.append(file_path)

    print("Matching files for today's date:")
    for match in matching_files:
        print(match)

    if error_files:
        print("\nFiles without recognizable date patterns:")
        for error in error_files:
            print(error)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter files by date in filenames.")
    parser.add_argument("--folder", required=True, help="Folder to search files recursively.")
    args = parser.parse_args()

    main(args.folder)
