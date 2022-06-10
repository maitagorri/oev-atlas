#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 2022

@author: maita

Attach RegioStaR7 classification to gem dataset.
"""


# Welcher Datensatz?
zipname = '20220425_fahrplaene_gesamtdeutschland_gtfs' # name of GTFS zipfile
# Welches RegioStaR-Jahr?
rstar_year = 19


# imports
import pandas as pd
import geopandas as gpd

# Welche Pfade?
out_dir = "../../data/processed/"
work_dir = "../../data/interim/"
raw_dir = "../../data/raw/"

# files
gem_path = out_dir + zipname + '.GEM.geojson'
rst_path = raw_dir + "bmdv/" + 'regiostar-referenzdateien.xlsx'
logfile = work_dir + zipname + ".log"

# excel laden
rst_df = pd.read_excel(rst_path,
              sheet_name="ReferenzGebietsstand20{}".format(rstar_year)
             )

# gem laden
gem_df = gpd.read_file(gem_path)

# merge
gem_df_merged = gem_df.merge(rst_df[["gem_{}".format(rstar_year),"RegioStaR7"]], left_on='AGS', right_on='gem_{}'.format(rstar_year), how='left')
pd.concat([
    gem_df_merged.loc[:,'GEM':'SFL'],
    gem_df_merged.loc[:, 'RegioStaR7'],
    gem_df_merged.loc[:,'halte.ges':'geometry']
], axis=1)
                              
n_no_match = len(gem_df_merged[gem_df_merged.RegioStaR7.isna()])

# write
gem_df.to_file(gem_path,driver="GeoJSON")

# log

with open(logfile, 'a') as f:
    f.write('## Verschnitt mit RegioStaR7\n')
    f.write('RegioStaR-Zuordnung aus Jahr 20{}\n'.format(rstar_year))
    f.write('{} Gemeinden nicht zugeordnet.'.format(n_no_match))
