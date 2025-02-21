from datamule import PremiumDownloader

# start at August 2015, where we have outstanding stock data for at least 5k companies
for year in range(2015, 2025):
    for month in range(8, 13) if year == 2015 else range(1, 13):
        downloader = PremiumDownloader()
        downloader.download_submissions(filing_date=(f'{year}-{str(month).zfill(2)}-01', f'{year}-{str(month).zfill(2)}-31'),
                                        submission_type=['3','4','5'],
                                        output_dir=f'holdings/{str(year)}_{str(month)}')

# we want to track common stock
# we need to account for multiple persons on the same form
# we need count after transaction

# we need total count: https://data.sec.gov/api/xbrl/frames/dei/EntityCommonStockSharesOutstanding/shares/CY2010Q1I.json
# there's also the us-gaap https://data.sec.gov/api/xbrl/frames/us-gaap/CommonStockSharesOutstanding/shares/CY2010Q4I.json

# should have like 5,500 a year.

# storage - 
# ~ 5000 or so csv files. each would be a couple hundred kb to a mb. fine for github. Should be under 1gb soft limit.