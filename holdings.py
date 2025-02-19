from datamule import Portfolio

portfolio = Portfolio('holdings')

# form 345 xml starts around 2005
portfolio.download_submissions(filing_date=('2005-01-01', '2024-12-31'),submission_type=['3','4','5'])

# we want to track common stock
# we need to account for multiple persons on the same form
# we need count after transaction
# we need total count: https://data.sec.gov/api/xbrl/frames/dei/EntityCommonStockSharesOutstanding/shares/CY2010Q1I.json