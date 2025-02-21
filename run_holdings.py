from datamule import Portfolio 
import pandas as pd
import os
import glob
import gender_guesser.detector as gender
from datamule import Book

def clean_keys(d):
    if not isinstance(d, dict):
        return d
    
    return {k.replace('-', '').replace(' ', ''): clean_keys(v) for k, v in d.items()}

def get_value(obj):
    """Helper to handle both direct values and nested value objects"""
    if isinstance(obj, dict):
        return obj.get('value', 0)
    return obj

def get_holdings(data):
    """Extract holdings from parsed Form 4 data"""
    direct = 0
    indirect = 0
    doc = data.get('ownershipDocument', {})

    relationship = doc.get('reportingOwner', {}).get('reportingOwnerRelationship', {})
    issuer_cik = doc.get('issuer', {}).get('issuerCik', {})
    issuer_name = doc.get('issuer', {}).get('issuerName', {})
    
    # Handle non-derivative table if it exists
    non_deriv = doc.get('nonDerivativeTable', {})
    
    # Get direct holdings from last transaction
    transactions = non_deriv.get('nonDerivativeTransaction', [])
    if transactions:
        if not isinstance(transactions, list):
            transactions = [transactions]
        if transactions:  # Check if list is not empty
            last_trans = transactions[-1]
            post_amounts = last_trans.get('postTransactionAmounts', {})
            shares = post_amounts.get('sharesOwnedFollowingTransaction', {})
            direct = float(get_value(shares) or 0)
    
    # Sum up indirect holdings
    holdings = non_deriv.get('nonDerivativeHolding', [])
    if holdings:
        if not isinstance(holdings, list):
            holdings = [holdings]
        for holding in holdings:
            ownership = holding.get('ownershipNature', {}).get('directOrIndirectOwnership', {})
            if get_value(ownership) == 'I':
                shares = holding.get('postTransactionAmounts', {}).get('sharesOwnedFollowingTransaction', {})
                indirect += float(get_value(shares) or 0)
    
    return {'direct': direct, 'indirect': indirect, 'issuer_cik': issuer_cik, 'issuer_name': issuer_name}  | relationship

def process_submission(submission):
    # return if reporting owner is a list (e.g. LPs)
    metadata = clean_keys(submission.metadata)
    if type(metadata['reportingowner']) == list:
        return None
    
    try:
        name = metadata['reportingowner']['ownerdata']['companyconformedname']
    except:
        name = metadata['reportingowner']['ownerdata']['conformedname']
        
    for document in submission.document_type('4'):
        parsed_data = document.parse()
        holdings = get_holdings(parsed_data)
        try:
            holdings['date'] = metadata['filingdate']
        except:
            holdings['date'] = metadata['filedasofdate']

        holdings['name'] = name
        holdings['accession'] = metadata['accessionnumber'].replace('-', '')
        return holdings

def process_portfolio(folder_name):
    """Process a single portfolio and save results to CSV"""
    portfolio = Portfolio(f'../holdings/{folder_name}')
    results = portfolio.process_submissions(process_submission)
    results = [x for x in results if x is not None]
    
    # save results to csv
    output_path = f'holdings/345/{folder_name}.csv'
    os.makedirs('holdings', exist_ok=True)  # Create holdings directory if it doesn't exist
    os.makedirs('holdings/345', exist_ok=True)  
    df = pd.DataFrame(results)
    df.to_csv(output_path, index=False)
    return output_path

def construct_holdings():
    # Get all folder names in ../holdings
    holdings_folders = [f for f in os.listdir('../holdings') if os.path.isdir(os.path.join('../holdings', f))]
    holdings_folders = ['archive']
    # Process each portfolio
    csv_files = []
    for folder in holdings_folders:
        print(f'Processing {folder}...')
        csv_file = process_portfolio(folder)
        csv_files.append(csv_file)
    
    # Combine all CSVs
    all_data = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        all_data.append(df)
    
    # Concatenate all dataframes and save final CSV
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv('holdings/345/single_reporting_owner_stock_holdings.csv', index=False)

    # cleaning (just send names to lowercase for now)
    final_df['name'] = final_df['name'].str.lower()
    final_df['issuer_name'] = final_df['issuer_name'].str.lower()

    # drop column otherText
    final_df = final_df.drop(columns=['otherText'])

    # split into company cik
    os.makedirs('holdings/345/ciks/', exist_ok=True)

    for cik in final_df['issuer_cik'].unique():
        cik_df = final_df[final_df['issuer_cik'] == cik]
        cik_df.to_csv(f'holdings/345/ciks/{cik}.csv', index=False)

def clean_name(full_name):
    try:
        return ({'first_name': full_name.split(' ')[1], 'last_name': full_name.split(' ')[0]})
    except:
        # skipping for now
        return ({'first_name': full_name, 'last_name': full_name})

def pred_gender_ethnicity():
    # Create gender detector
    d = gender.Detector(case_sensitive=False)
    
    # Get all CSV files in the ciks directory
    for file in glob.glob('holdings/345/ciks/*.csv'):
        print(f'Processing {file}...')
        df = pd.read_csv(file)
        
        # Use clean_name to get first names from full names
        names_df = pd.DataFrame([clean_name(name) for name in df['name']])
        
        # Predict gender using first names
        df['predicted_gender'] = [d.get_gender(first_name) for first_name in names_df['first_name']]

        gender_mapping = {
            'male': 'male',
            'mostly_male': 'male',
            'female': 'female',
            'mostly_female': 'female',
            'andy': 'male',  # You can change this default
            'unknown': 'male'  # You can change this default
        }

        df['predicted_gender'] = df['predicted_gender'].map(gender_mapping)
        
        # Save the updated dataframe back to CSV
        df.to_csv(file, index=False)
        


# modify to write to holdings/commonstock.csv
def dei_callback(data):
    for i, key in enumerate(data.keys()):
        df = pd.DataFrame(data[key]['data'])
        # rename accn -> accession
        df.rename(columns={'accn': 'accession'}, inplace=True)
        # rename end -> date
        df.rename(columns={'end': 'date'}, inplace=True)
        # rename value -> shares
        df.rename(columns={'val': 'shares'}, inplace=True)

        # remove dashes from accession and date
        df['accession'] = df['accession'].str.replace('-', '')
        df['date'] = df['date'].str.replace('-', '')

        df.to_csv('holdings/commonstock.csv', 
                  index=False,
                  mode='w' if i == 0 else 'a',
                  header=i == 0)

def gaap_callback(data):
    for key in data.keys():
        df = pd.DataFrame(data[key]['data'])
        # rename accn -> accession
        df.rename(columns={'accn': 'accession'}, inplace=True)
        # rename end -> date
        df.rename(columns={'end': 'date'}, inplace=True)
        # rename value -> shares
        df.rename(columns={'val': 'shares'}, inplace=True)

        # remove dashes from accession and date
        df['accession'] = df['accession'].str.replace('-', '')
        df['date'] = df['date'].str.replace('-', '')

        df.to_csv('holdings/commonstock.csv',
                  index=False,
                  mode='a',
                  header=False)

def get_stocks():
    book = Book()
    book.XBRLRetriever.set_limiter(4)
    periods = []
    
    # Generate periods from 2010 to 2025
    years = range(2008, 2025)
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    
    for year in years:
        for quarter in quarters:
            periods.append(f'CY{year}{quarter}I')
    
    book.filter_xbrl('dei', 'EntityCommonStockSharesOutstanding', 'shares', periods, '>', 0,callback=dei_callback)
    book.filter_xbrl('us-gaap', 'CommonStockSharesOutstanding', 'shares', periods, '>', 0,callback=gaap_callback)

def merge_stocks():
    """Add total_stock to CIK files by matching with most recent quarterly report"""
    # Read commonstock data
    commonstock_df = pd.read_csv('holdings/commonstock.csv')
    
    # Process each CIK file
    for file_path in glob.glob('holdings/345/ciks/*.csv'):
        print(f'Processing {file_path}...')
        
        # Read the CIK file
        cik_df = pd.read_csv(file_path)
        
        # Drop any existing total_stock columns
        cik_df = cik_df[[col for col in cik_df.columns if 'total_stock' not in col]]
        
        # Get the CIK number
        cik = cik_df['issuer_cik'].iloc[0]
        
        # Filter commonstock data for this CIK
        cik_stock_data = commonstock_df[commonstock_df['cik'] == int(cik)]
        
        # Sort both dataframes by date
        cik_df = cik_df.sort_values('date')
        cik_stock_data = cik_stock_data.sort_values('date')
        
        # Merge using merge_asof to get most recent prior stock report
        merged_df = pd.merge_asof(cik_df, 
                                cik_stock_data[['date', 'shares']], 
                                on='date',
                                direction='backward')
        
        # Rename shares to total_stock
        merged_df = merged_df.rename(columns={'shares': 'total_stock'})
        
        # Save
        merged_df.to_csv(file_path, index=False)

if __name__ == "__main__":
    #construct_holdings()
    #pred_gender_ethnicity()
    get_stocks()
    merge_stocks()
    pass