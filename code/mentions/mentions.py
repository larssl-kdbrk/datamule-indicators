import pandas as pd
import os
from datetime import datetime, timedelta
from .utils import create_text_fragment_url

def create_indicator(format, category, indicator, components, interval, query_list, period):
    # Create directory structure once
    directory_path = os.path.join("indicators", format, category, indicator)
    os.makedirs(directory_path, exist_ok=True)
    
    # Load filer metadata once (outside the loop)
    filer_metadata = pd.read_csv('https://github.com/john-friedman/datamule-data/raw/refs/heads/master/data/filer_metadata/listed_filer_metadata.csv.gz',
                                compression='gzip',
                                usecols=['cik', 'ownerOrg', 'name'])  # Added 'name' to the columns
    
    # Remove leading numbers from ownerOrg once
    filer_metadata['ownerOrg'] = filer_metadata['ownerOrg'].str.replace(r'^\d+\s', '', regex=True)
    
    # Define time periods once
    now = datetime.now()
    current_period_start = now - timedelta(days=period)
    previous_period_start = current_period_start - timedelta(days=period)
    
    # Initialize DataFrames to collect results
    overview_data = []
    sector_comparison_data = []
    references_data = []
    
    # Process each component once
    for idx, component in enumerate(components):
        # Extract component name once
        component_name = os.path.basename(component).split('_')[-1].split('.')[0]
        print(f"Processing component: {component_name}")
        
        # Load component data once
        df = pd.read_csv(component, compression='gzip')
        df['filing_date'] = pd.to_datetime(df['filing_date'])
        df['component'] = component_name
        
        # 1. Create overview data
        overview_df = df[['filing_date']].copy()
        overview_df['count'] = 1
        overview_df = overview_df.set_index('filing_date')
        overview_df = overview_df.resample(interval).sum()
        overview_df['component'] = component_name
        overview_data.append(overview_df)
        
        # 2. Create sector comparison data
        sector_df = df[['filing_date', 'cik']].copy()
        sector_df = sector_df.merge(filer_metadata, on='cik', how='left')
        
        # Add period column using vectorized operations
        sector_df['period'] = None
        sector_df.loc[(sector_df['filing_date'] >= current_period_start) & (sector_df['filing_date'] < now), 'period'] = 0
        sector_df.loc[(sector_df['filing_date'] >= previous_period_start) & (sector_df['filing_date'] < current_period_start), 'period'] = 1
        
        # Filter to only include rows with valid periods
        sector_df = sector_df.dropna(subset=['period'])
        
        # Count records by sector and period
        sector_df['count'] = 1
        sector_counts = sector_df.groupby(['ownerOrg', 'period'])['count'].sum().reset_index()
        sector_counts['component'] = component_name
        sector_comparison_data.append(sector_counts)
        
        # 3. Create references data
        references_df = df[df['filing_date'] >= current_period_start].copy()
        
        # Merge with filer_metadata to add ownerOrg and name
        references_df = references_df.merge(filer_metadata[['cik', 'ownerOrg', 'name']], 
                                            on='cik', how='left')
        
        # Vectorize URL creation instead of using apply
        references_df['url'] = (
            "https://www.sec.gov/Archives/edgar/data/" + 
            references_df['cik'].astype(str) + "/" + 
            references_df['accession_number'].str.replace('-', '') + "/" + 
            references_df['filename']
        )
        
        # vectorize later
        new_urls = []
        for _, row in references_df.iterrows():
            # Use the first query list for testing purposes
            new_url = create_text_fragment_url(row['url'], query_list[idx])
            new_urls.append(new_url)

        # Then assign all at once
        references_df['url'] = new_urls
        references_data.append(references_df)
    
    # Combine and write overview data once
    if overview_data:
        combined_overview = pd.concat(overview_data)
        combined_overview.to_csv(os.path.join(directory_path, "overview.csv"), index=True)
    
    # Combine and write sector comparison data once
    if sector_comparison_data:
        combined_sector = pd.concat(sector_comparison_data)
        combined_sector.to_csv(os.path.join(directory_path, "sector_comparison.csv"), index=False)
    
    # Combine and write references data once
    if references_data:
        combined_references = pd.concat(references_data)
        # Select only the columns we want for references.csv
        combined_references = combined_references[['filing_date', 'component', 'url', 'ownerOrg', 'name']]
        combined_references.to_csv(os.path.join(directory_path, "references.csv"), index=False)