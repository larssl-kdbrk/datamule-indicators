from datamule import Book
import pandas as pd
import os

def search_filings(search_term, output_path, start_year=2001, end_year=2024,month_flag=True):
    print(f"Searching for '{search_term}'")
    def search_callback(data):
        # Transform the data
        transformed = [
            {**{k: v for k, v in item.items() if k != 'ciks'}, 'cik': cik}
            for item in data
            for cik in item['ciks']
        ]
        
        if not transformed:
            return
            
        df = pd.DataFrame(transformed)
        df.to_csv(output_path, 
                 mode='a', 
                 header=not os.path.exists(output_path), 
                 index=False)

    for year in range(start_year, end_year + 1):
        if month_flag:
            for month in range(1, 13):
                book = Book(filing_date=(f"{year}-{str(month).zfill(2)}-01",f"{year}-{str(month).zfill(2)}-31"))
                book.filter_text(text=search_term, callback=search_callback)
        else:
            book = Book(filing_date=(f"{year}-01-01", f"{year}-12-31"))
            book.filter_text(text=search_term, callback=search_callback)
