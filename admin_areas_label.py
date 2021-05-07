#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 7

@author: maita

Creates label geojson for the VG250 admin geometries 
"""

import geopandas as gpd

# define the points layers
out_path = "/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"
pointfiles = {"all":out_path + "nstops.csv",

              "fv":out_path + "fv.nstops.csv"}

###############
    # shapes
###############
    
# define the area layers
geo_path = "/home/maita/Nextcloud/Documents/Work/Gap_Map/raw/geo/"
admin_area_path = geo_path + "vg250-ew_12-31.utm32s.shape.ebenen/vg250-ew_ebenen_1231/"
shapefiles = {"gem":admin_area_path + "VG250_GEM.shp",
              "kre":admin_area_path + "VG250_KRS.shp",
              "lan":admin_area_path + "VG250_LAN.shp"}

centroids_gdf = gpd.GeoDataFrame(columns = ["label", "level", "centroid"], geometry = "centroid", crs = "epsg:3857")
for level in shapefiles.keys():
    print("labelling " + level + "...")
    # Read in admin areas
    area_path = shapefiles[level]
    area_gdf = gpd.read_file(area_path)
    # fix projection
    area_gdf = area_gdf.to_crs("epsg:3857")
    area_gdf.AGS = area_gdf.AGS.astype(int) # to compare to other sources, get reasonable type for AGS
    # select correct shapes, and dissolve them by AGS/GEN
    agg_gdf = area_gdf[(area_gdf.KFL>0)][["AGS","GEN","geometry"]].dissolve(by="AGS")
    agg_gdf["centroid"] = agg_gdf.centroid
    agg_gdf["level"]  = level
    agg_gdf.rename(columns = {'GEN':'label'}, inplace = True)
    this_centroids_gdf = gpd.GeoDataFrame(agg_gdf[["label", "level", "centroid"]], geometry = 'centroid', crs="epsg:3857") 
    centroids_gdf = centroids_gdf.append(this_centroids_gdf)

centroids_gdf.to_file(out_path + "labels.3857.geojson",driver="GeoJSON")
print("Wrote labels.3857.geojson")