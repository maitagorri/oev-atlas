#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 12:17:20 2021

@author: maita
"""

import pandas as pd
import geopandas as gpd
import ndjson

# load the grid layers
areas = {"land":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/lan.json",
        "kreis":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/halte_pro_kreis_dedupe.geojson",
        "gemeinde":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/halte_pro_gemeinde_dedupe.geojson"}



for a in areas.keys():
    df = gpd.read_file(areas[a])[['GEN', 'BEZ', 'n.rs', 'n.fv', 'n.nv', 'n','n.ewz', 'n.sfl', 'geometry']]
    df.to_file("/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"+a + "_relevant.geojson",driver="GeoJSON")