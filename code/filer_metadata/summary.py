import csv
import gzip
import urllib.request
import os
import json
import re
from collections import Counter

def aggregate_filer_metadata(output_dir):
    """
    Downloads and aggregates metadata for listed and unlisted filers.

    This function:
    1. Downloads data for listed and unlisted companies
    2. Creates aggregated CSV files for various columns
    3. Generates summary statistics
    4. Saves all results in the specified output directory

    Args:
        output_dir (str): Directory where output files will be saved
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    listed_url = 'https://github.com/john-friedman/datamule-data/raw/master/data/filer_metadata/listed_filer_metadata.csv.gz'
    unlisted_url = 'https://github.com/john-friedman/datamule-data/raw/master/data/filer_metadata/unlisted_filer_metadata.csv.gz'

    def load_github_csv_gz(url):
        print(f'Downloading data from {url}...')
        response = urllib.request.urlopen(url)
        content = response.read()
        decompressed_data = gzip.decompress(content).decode('utf-8')
        reader = csv.DictReader(decompressed_data.splitlines())
        data = []
        for row in reader:
            data.append(row)
        print(f'Downloaded {len(data)} rows')
        return data

    def clean_text(text):
        if not text:
            return ''
        return text

    def clean_zip_code(zip_code):
        if not zip_code:
            return ''
        return zip_code

    def aggregate_by_column(data, column_name):
        counter = Counter()
        for row in data:
            value = row.get(column_name, '').strip()
            if column_name == 'business_zipCode':
                value = clean_zip_code(value)
            if value:
                counter[value] += 1
        result = []
        for value, count in counter.most_common():
            result.append({column_name: value, 'count': count})
        return result

    def write_aggregated_csv(data, output_file, columns):
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            for row in data:
                writer.writerow([row[col] for col in columns])
    listed_data = load_github_csv_gz(listed_url)
    unlisted_data = load_github_csv_gz(unlisted_url)
    for prefix, data in [('listed', listed_data), ('unlisted', unlisted_data)]:
        print(f'Processing {prefix} data...')
        city_aggregate = aggregate_by_column(data, 'business_city')
        write_aggregated_csv(city_aggregate, os.path.join(output_dir, f'{prefix}_business_city_aggregate.csv'), ['business_city', 'count'])
        country_data = []
        for row in data:
            country_desc = clean_text(row.get('business_stateOrCountryDescription', '').strip())
            state_country = clean_text(row.get('business_stateOrCountry', '').strip())
            country = ''
            if ',' in country_desc:
                country = country_desc.split(',')[1].strip()
            country = 'usa' if state_country and country and (state_country == country) else country
            if country:
                country_data.append({'country': country})
        country_aggregate = aggregate_by_column(country_data, 'country')
        write_aggregated_csv(country_aggregate, os.path.join(output_dir, f'{prefix}_country_aggregate.csv'), ['country', 'count'])
        usa_rows = []
        for row in data:
            country_desc = clean_text(row.get('business_stateOrCountryDescription', '').strip())
            state_country = clean_text(row.get('business_stateOrCountry', '').strip())
            is_usa = False
            is_usa = True if state_country and state_country == country_desc else False
            if is_usa:
                zip_code = clean_zip_code(row.get('business_zipCode', '').strip())
                if zip_code:
                    usa_rows.append({'zip_code': zip_code})
        zip_counter = Counter()
        for row in usa_rows:
            zip_counter[row['zip_code']] += 1
        zip_aggregate = []
        for zip_code, count in zip_counter.most_common():
            zip_aggregate.append({'zip_code': zip_code, 'count': count})
        write_aggregated_csv(zip_aggregate, os.path.join(output_dir, f'{prefix}_zip_code_aggregate.csv'), ['zip_code', 'count'])
        for column in ['category', 'entityType', 'fiscalYearEnd', 'insiderTransactionForIssuerExists', 'insiderTransactionForOwnerExists', 'ownerOrg', 'stateOfIncorporationDescription']:
            print(f'  Aggregating by {column}...')
            column_aggregate = aggregate_by_column(data, column)
            write_aggregated_csv(column_aggregate, os.path.join(output_dir, f'{prefix}_{column}_aggregates.csv'), [column, 'count'])
        sic_data = []
        for row in data:
            sic = row.get('sic', '').strip()
            sic_desc = clean_text(row.get('sicDescription', '').strip())
            if sic:
                sic_data.append({'sic': sic, 'sicDescription': sic_desc if sic_desc else 'unknown'})
        sic_counter = Counter()
        for row in sic_data:
            sic_counter[row['sic']] += 1
        sic_aggregate = []
        for sic, count in sic_counter.most_common():
            description = next((row['sicDescription'] for row in sic_data if row['sic'] == sic), 'unknown')
            sic_aggregate.append({'sic': sic, 'sicDescription': description, 'count': count})
        write_aggregated_csv(sic_aggregate, os.path.join(output_dir, f'{prefix}_sic_aggregates.csv'), ['sic', 'sicDescription', 'count'])

    summary = {
    'listed': {
        'ein_count': sum(1 for row in listed_data if row.get('ein', '').strip()),
        'phone_count': sum(1 for row in listed_data if row.get('phone', '').strip()),
        'total_count': len(listed_data)
    },
    'unlisted': {
        'ein_count': sum(1 for row in unlisted_data if row.get('ein', '').strip()),
        'phone_count': sum(1 for row in unlisted_data if row.get('phone', '').strip()),
        'total_count': len(unlisted_data)
    }
}

    with open(os.path.join(output_dir, 'summary.json'), 'w') as f:
        json.dump(summary, f, indent=4)
    print('Analysis complete! Files generated in', output_dir)