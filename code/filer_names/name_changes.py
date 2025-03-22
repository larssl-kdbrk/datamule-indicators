import csv
import gzip
import urllib.request
import re
from datetime import datetime, timedelta
import os

def generate_name_changes_indicators(output_dir):
    """
    Generates datasets tracking all company name changes over time.
    
    This function:
    1. Downloads data for listed and unlisted companies
    2. Tracks daily counts of companies changing their names
    3. Saves results as CSV files in the specified output directory
    
    Args:
        output_dir (str): Directory where output CSV files will be saved
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # URLs for the datasets
    listed_url = "https://github.com/john-friedman/datamule-data/raw/master/data/filer_metadata/listed_filer_names.csv.gz"
    unlisted_url = "https://github.com/john-friedman/datamule-data/raw/master/data/filer_metadata/unlisted_filer_names.csv.gz"

    # Function to download and load the gzipped CSV file
    def load_github_csv_gz(url):
        # Download the gzipped file
        print(f"Downloading data from {url}...")
        response = urllib.request.urlopen(url)
        content = response.read()
        
        # Decompress the file
        decompressed_data = gzip.decompress(content).decode('utf-8')
        
        # Parse CSV data
        reader = csv.DictReader(decompressed_data.splitlines())
        data = []
        for row in reader:
            data.append(row)
        
        print(f"Downloaded and parsed {len(data)} records.")
        return data

    # Load and process listed filers data
    print("Processing listed filers data...")
    listed_data = load_github_csv_gz(listed_url)

    # Filter out rows with null start_date
    listed_filtered = [row for row in listed_data if row['start_date'] and row['start_date'].strip()]
    print(f"After filtering null start dates: {len(listed_filtered)} listed filer records.")

    # Load and process unlisted filers data
    print("Processing unlisted filers data...")
    unlisted_data = load_github_csv_gz(unlisted_url)

    # Filter out rows with null start_date
    unlisted_filtered = [row for row in unlisted_data if row['start_date'] and row['start_date'].strip()]
    print(f"After filtering null start dates: {len(unlisted_filtered)} unlisted filer records.")

    # Process listed filers for daily name changes
    print("Tracking listed filer name changes...")

    # Initialize dictionaries to store daily counts
    listed_daily_name_changes = {}

    # Group by CIK to track company name changes
    listed_by_cik = {}
    for row in listed_filtered:
        cik = row['cik']
        if cik not in listed_by_cik:
            listed_by_cik[cik] = []
        listed_by_cik[cik].append(row)

    # Count name changes by date
    for cik, records in listed_by_cik.items():
        # Sort by start_date to ensure chronological order
        records.sort(key=lambda x: x['start_date'])
        
        # Track name changes
        for i in range(1, len(records)):
            # The start_date of the new record is when the name change occurred
            change_date = records[i]['start_date']
            
            # Increment the count for this date
            if change_date not in listed_daily_name_changes:
                listed_daily_name_changes[change_date] = 0
            listed_daily_name_changes[change_date] += 1

    # Process unlisted filers for daily name changes
    print("Tracking unlisted filer name changes...")

    # Initialize dictionaries to store daily counts
    unlisted_daily_name_changes = {}

    # Group by CIK to track company name changes
    unlisted_by_cik = {}
    for row in unlisted_filtered:
        cik = row['cik']
        if cik not in unlisted_by_cik:
            unlisted_by_cik[cik] = []
        unlisted_by_cik[cik].append(row)

    # Count name changes by date
    for cik, records in unlisted_by_cik.items():
        # Sort by start_date to ensure chronological order
        records.sort(key=lambda x: x['start_date'])
        
        # Track name changes
        for i in range(1, len(records)):
            # The start_date of the new record is when the name change occurred
            change_date = records[i]['start_date']
            
            # Increment the count for this date
            if change_date not in unlisted_daily_name_changes:
                unlisted_daily_name_changes[change_date] = 0
            unlisted_daily_name_changes[change_date] += 1

    # Write results to CSV files
    print("Writing results to CSV files...")

    # Write daily name changes for listed filers
    with open(os.path.join(output_dir, 'listed_rebranding_daily.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'count'])
        for date in sorted(listed_daily_name_changes.keys()):
            writer.writerow([date, listed_daily_name_changes[date]])

    # Write daily name changes for unlisted filers
    with open(os.path.join(output_dir, 'unlisted_rebranding_daily.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'count'])
        for date in sorted(unlisted_daily_name_changes.keys()):
            writer.writerow([date, unlisted_daily_name_changes[date]])

    print("Analysis complete! Files generated in", output_dir)
    print("1. listed_rebranding_daily.csv")
    print("2. unlisted_rebranding_daily.csv")

