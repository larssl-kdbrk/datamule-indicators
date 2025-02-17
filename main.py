from mentions import search_filings
import os
import shutil

shutil.rmtree("mentions", ignore_errors=True)
os.makedirs("mentions", exist_ok=True)

# economic
search_filings("tariffs", "mentions/tariffs.csv")
search_filings("recession", "mentions/recession.csv")
search_filings("inflation", "mentions/inflation.csv")
search_filings("interest rates", "mentions/interest_rates.csv")
search_filings("cryptocurrency", "mentions/cryptocurrency.csv")
search_filings("labor shortage", "mentions/labor_shortage.csv")


# terror
search_filings("hamas", "mentions/hamas.csv")
search_filings("islamic state", "mentions/islamic_state.csv")

# country
search_filings("china", "mentions/china.csv")
search_filings("japan", "mentions/japan.csv")
search_filings("germany", "mentions/germany.csv")
search_filings("india", "mentions/india.csv")
search_filings("france", "mentions/france.csv")
search_filings("russia", "mentions/russia.csv")
search_filings("canada", "mentions/canada.csv")
search_filings("italy", "mentions/italy.csv")
search_filings("brazil", "mentions/brazil.csv")
search_filings("australia", "mentions/australia.csv")
search_filings("south korea", "mentions/south_korea.csv")
search_filings("mexico", "mentions/mexico.csv")
search_filings("spain", "mentions/spain.csv")


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

# health
search_filings("pandemic", "mentions/pandemic.csv")
search_filings("vaccine", "mentions/vaccine.csv")
search_filings("mental health", "mentions/mental_health.csv")

# risks
search_filings("political instability", "mentions/political_instability.csv")
search_filings("water scarcity", "mentions/water_scarcity.csv")
