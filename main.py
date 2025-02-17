from mentions import search_filings
import os
import shutil
from time import sleep

#shutil.rmtree("mentions", ignore_errors=True)
#os.makedirs("mentions", exist_ok=True)

# economic
#search_filings("tariffs", "mentions/tariffs.csv")
#search_filings("recession", "mentions/recession.csv")
#search_filings("cryptocurrency", "mentions/cryptocurrency.csv",month_flag=False)
#search_filings("labor shortage", "mentions/labor_shortage.csv")


# terror
#search_filings("hamas", "mentions/hamas.csv")
search_filings("houthi", "mentions/houthi.csv",month_flag=False)

# disasters
search_filings("earthquake", "mentions/earthquake.csv")
search_filings("hurricane", "mentions/hurricane.csv")
search_filings("wildfire", "mentions/wildfire.csv")

# technology
search_filings("artificial intelligence", "mentions/artificial_intelligence.csv")
search_filings("cybersecurity", "mentions/cybersecurity.csv")
search_filings("5G", "mentions/5G.csv")
search_filings("automation", "mentions/automation.csv")

# environmental
search_filings("climate change", "mentions/climate_change.csv")
search_filings("renewable energy", "mentions/renewable_energy.csv")
search_filings("carbon emissions", "mentions/carbon_emissions.csv")
search_filings("ESG", "mentions/ESG.csv")
search_filings("sustainability", "mentions/sustainability.csv")

# regulatory
search_filings("antitrust", "mentions/antitrust.csv")
search_filings("GDPR", "mentions/GDPR.csv")
search_filings("sanctions", "mentions/sanctions.csv")

# risks
search_filings("political instability", "mentions/political_instability.csv")
search_filings("water scarcity", "mentions/water_scarcity.csv")

# health
search_filings("pandemic", "mentions/pandemic.csv")
search_filings("vaccine", "mentions/vaccine.csv")
search_filings("mental health", "mentions/mental_health.csv")

# country
# search_filings("canada", "mentions/canada.csv")
# search_filings("mexico", "mentions/mexico.csv")
# search_filings("australia", "mentions/australia.csv")