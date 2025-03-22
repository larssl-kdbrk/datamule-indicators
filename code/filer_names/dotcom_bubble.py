import csv
import gzip
import urllib.request
import re
from datetime import datetime, timedelta
import os

def generate_dotcom_bubble_indicators(output_dir):
    """
    Generates datasets tracking the usage of "COM" in company names over time.
    
    This function:
    1. Downloads data for listed and unlisted companies
    2. Tracks daily counts of companies with "COM" in their names
    3. Records name changes between COM and non-COM names
    4. Saves all results as CSV files in the specified output directory
    
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
        response = urllib.request.urlopen(url)
        content = response.read()
        
        # Decompress the file
        decompressed_data = gzip.decompress(content).decode('utf-8')
        
        # Parse CSV data
        reader = csv.DictReader(decompressed_data.splitlines())
        data = []
        for row in reader:
            data.append(row)
        
        return data

    # Function to check if a company name contains "COM" as a separate word
    def has_com_in_name(name):
        # Convert to uppercase for case-insensitive matching
        name = name.upper()
        # Match "COM" as a word boundary, including ".COM"
        pattern = r'\b(COM|\.COM)\b'
        return bool(re.search(pattern, name))

    # Function to generate date range between start and end dates
    def generate_date_range(start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        dates = []
        current = start
        while current < end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        
        return dates

    # Load and process listed filers data
    print("Downloading and processing listed filers data...")
    listed_data = load_github_csv_gz(listed_url)

    # Filter out rows with null start_date
    listed_filtered = [row for row in listed_data if row['start_date'] and row['start_date'].strip()]

    # Load and process unlisted filers data
    print("Downloading and processing unlisted filers data...")
    unlisted_data = load_github_csv_gz(unlisted_url)

    # Filter out rows with null start_date
    unlisted_filtered = [row for row in unlisted_data if row['start_date'] and row['start_date'].strip()]

    # Process listed filers for daily counts and name changes
    print("Processing listed filers for daily counts and name changes...")

    # Initialize dictionaries to store daily counts
    listed_daily_counts = {}

    # Initialize lists to store name changes
    listed_com_to_non = []
    listed_non_to_com = []
    listed_com_to_com = []

    # Group by CIK to track company name changes
    listed_by_cik = {}
    for row in listed_filtered:
        cik = row['cik']
        if cik not in listed_by_cik:
            listed_by_cik[cik] = []
        listed_by_cik[cik].append(row)

    # Sort each company's records by start_date
    for cik, records in listed_by_cik.items():
        records.sort(key=lambda x: x['start_date'])
        
        # Check for name changes
        for i in range(1, len(records)):
            prev_record = records[i-1]
            curr_record = records[i]
            
            prev_has_com = has_com_in_name(prev_record['name'])
            curr_has_com = has_com_in_name(curr_record['name'])
            
            # Record name changes
            if prev_has_com and not curr_has_com:
                listed_com_to_non.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
            elif not prev_has_com and curr_has_com:
                listed_non_to_com.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
            elif prev_has_com and curr_has_com:
                listed_com_to_com.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
        
        # Generate daily counts
        for record in records:
            if not record['end_date'] or not record['end_date'].strip():
                # If no end date, use today's date
                record['end_date'] = datetime.now().strftime("%Y-%m-%d")
            
            # Check if the name has "COM"
            if has_com_in_name(record['name']):
                # Add to daily counts for each day in the range
                # (excluding the change date)
                dates = generate_date_range(record['start_date'], record['end_date'])
                
                # Skip the first date (change date)
                for date in dates[1:]:
                    if date not in listed_daily_counts:
                        listed_daily_counts[date] = 0
                    listed_daily_counts[date] += 1

    # Process unlisted filers for daily counts and name changes
    print("Processing unlisted filers for daily counts and name changes...")

    # Initialize dictionaries to store daily counts
    unlisted_daily_counts = {}

    # Initialize lists to store name changes
    unlisted_com_to_non = []
    unlisted_non_to_com = []
    unlisted_com_to_com = []

    # Group by CIK to track company name changes
    unlisted_by_cik = {}
    for row in unlisted_filtered:
        cik = row['cik']
        if cik not in unlisted_by_cik:
            unlisted_by_cik[cik] = []
        unlisted_by_cik[cik].append(row)

    # Sort each company's records by start_date
    for cik, records in unlisted_by_cik.items():
        records.sort(key=lambda x: x['start_date'])
        
        # Check for name changes
        for i in range(1, len(records)):
            prev_record = records[i-1]
            curr_record = records[i]
            
            prev_has_com = has_com_in_name(prev_record['name'])
            curr_has_com = has_com_in_name(curr_record['name'])
            
            # Record name changes
            if prev_has_com and not curr_has_com:
                unlisted_com_to_non.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
            elif not prev_has_com and curr_has_com:
                unlisted_non_to_com.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
            elif prev_has_com and curr_has_com:
                unlisted_com_to_com.append({
                    'cik': cik,
                    'original_name': prev_record['name'],
                    'new_name': curr_record['name'],
                    'date': curr_record['start_date']
                })
        
        # Generate daily counts
        for record in records:
            if not record['end_date'] or not record['end_date'].strip():
                # If no end date, use today's date
                record['end_date'] = datetime.now().strftime("%Y-%m-%d")
            
            # Check if the name has "COM"
            if has_com_in_name(record['name']):
                # Add to daily counts for each day in the range
                # (excluding the change date)
                dates = generate_date_range(record['start_date'], record['end_date'])
                
                # Skip the first date (change date)
                for date in dates[1:]:
                    if date not in unlisted_daily_counts:
                        unlisted_daily_counts[date] = 0
                    unlisted_daily_counts[date] += 1

    # Write results to CSV files
    print("Writing results to CSV files...")

    # Write daily counts for listed filers
    with open(os.path.join(output_dir, 'listed_filer_dotcom_names_daily.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'count'])
        for date in sorted(listed_daily_counts.keys()):
            writer.writerow([date, listed_daily_counts[date]])

    # Write daily counts for unlisted filers
    with open(os.path.join(output_dir, 'unlisted_filer_dotcom_names_daily.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'count'])
        for date in sorted(unlisted_daily_counts.keys()):
            writer.writerow([date, unlisted_daily_counts[date]])

    # Write name changes for listed filers
    with open(os.path.join(output_dir, 'listed_com_to_non.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in listed_com_to_non:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    with open(os.path.join(output_dir, 'listed_non_to_com.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in listed_non_to_com:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    with open(os.path.join(output_dir, 'listed_com_to_com.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in listed_com_to_com:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    # Write name changes for unlisted filers
    with open(os.path.join(output_dir, 'unlisted_com_to_non.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in unlisted_com_to_non:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    with open(os.path.join(output_dir, 'unlisted_non_to_com.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in unlisted_non_to_com:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    with open(os.path.join(output_dir, 'unlisted_com_to_com.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cik', 'original_name', 'new_name', 'date'])
        for change in unlisted_com_to_com:
            writer.writerow([change['cik'], change['original_name'], change['new_name'], change['date']])

    print("Analysis complete! Files generated in", output_dir)
    print("1. listed_filer_dotcom_names_daily.csv")
    print("2. unlisted_filer_dotcom_names_daily.csv")
    print("3. listed_com_to_non.csv")
    print("4. listed_non_to_com.csv")
    print("5. listed_com_to_com.csv")
    print("6. unlisted_com_to_non.csv")
    print("7. unlisted_non_to_com.csv")
    print("8. unlisted_com_to_com.csv")
