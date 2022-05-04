import zipfile
import pandas as pd

# Parameter
zippath = "../../data/raw/delfi/20220419_fahrplaene_gesamtdeutschland_gtfs.zip"

# Setup
file = zipfile.ZipFile(zippath)

# Agenturen
agencies = pd.read_csv(zipfile.ZipFile(zippath
                        ).open("agency.txt"))

fv_agencies = agencies[agencies.agency_name.str.contains("Fernverkehr|Flix")]
print("Agenturen im Fernverkehr:\n", fv_agencies.agency_name)

# Routen
routes = pd.read_csv(file.open("routes.txt"))

fv_routes = routes[routes.agency_id.isin(fv_agencies.agency_id)]
print("Anzahl Ferverkehrsrouten: " + str(len(fv_routes)))

# Ausschreiben
outpath = zippath.rstrip('.zip').replace('raw/delfi','interim') + "_fv-routes.csv"
fv_routes.to_csv(outpath)
