import zipfile
import pandas as pd

# Parameter
zippath = "../../data/raw/delfi/20220419_fahrplaene_gesamtdeutschland_gtfs.zip"

# Setup
file = zipfile.ZipFile(zippath)

# Agenturen
agencies = pd.read_csv(zipfile.ZipFile(zippath
                        ).open("agency.txt"))

fz_agencies = agencies[agencies.agency_name.str.contains("Fernverkehr|Flix|Eurocity|SBB|Hamburg-Köln-Express|SNCF|Trenitalia|Österreichische") & 
                       ~agencies.agency_name.str.contains("Codesharing|Bus")
                      ]
print("Agenturen im Fernverkehr:\n", fz_agencies.agency_name)

# Bus-Agenturen
fb_agencies = agencies[agencies.agency_name.str.contains("Fernverkehr|Flix|Eurocity|SBB|Hamburg-Köln-Express|SNCF|Trenitalia|Österreichische") & 
                       agencies.agency_name.str.contains("Bus")
                      ]
print("Fernbus-Agenturen:\n", fb_agencies.agency_name)

# Routen
routes = pd.read_csv(file.open("routes.txt"))

fz_routes = routes[routes.agency_id.isin(fz_agencies.agency_id)]
print("Anzahl Fernzugrouten: " + str(len(fz_routes)))

fb_routes = routes[routes.agency_id.isin(fb_agencies.agency_id)]
print("Anzahl Fernbusrouten: " + str(len(fb_routes)))



# Ausschreiben
fz_outpath = zippath.rstrip('.zip').replace('raw/delfi','interim') + "_fz-routes.csv"
fz_routes.to_csv(fz_outpath)

fb_outpath = zippath.rstrip('.zip').replace('raw/delfi','interim') + "_fb-routes.csv"
fb_routes.to_csv(fb_outpath)

