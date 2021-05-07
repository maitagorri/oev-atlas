#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 20:51:56 2021

@author: maita

Once counts are aggregated by station, and saved in tables, this code lets you
combine this information with information about administrative areas and their 
geographies.
"""


import pandas as pd
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

# define additional information tables
sfl_tables= {"gem":geo_path + "Tabelle_Siedlungsflaeche_Gemeinde.csv",
              "kre":geo_path + "Tabelle_Siedlungsflaeche_Kreis.csv",
              "lan":geo_path + "Tabelle_Siedlungsflaeche_Land.csv"}

for level in shapefiles.keys():
    # Read in admin areas
    area_path = shapefiles[level]
    area_gdf = gpd.read_file(area_path)
    # fix projection
    area_gdf = area_gdf.to_crs("epsg:3857")
    area_gdf.AGS = area_gdf.AGS.astype(int) # to compare to other sources, get reasonable type for AGS
  
    # get counts of stops in all shapes associated with each AGS
    agg_counts_df = area_gdf[["AGS"]].drop_duplicates()
    for scope in pointfiles.keys():
        print("Counting "+scope+" in "+level)
        # read counts per station, with point locations
        ncounts_df = pd.read_csv(pointfiles[scope])
        ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
                                       geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
                                       crs="epsg:4326").to_crs("epsg:3857")
        ## get sum of stuff in each AGS
        agg_counts_df = gpd.sjoin(area_gdf[["AGS","geometry"]], ncounts_gdf[["n","geometry"]], how="left", op="contains" # spatial join
                            )[["AGS","n"]
                            ].groupby("AGS").sum().rename({"n":"n."+scope},axis=1
                            ).merge(agg_counts_df, how="right", on="AGS") # keep AGS that have counts in them

    # Aggregate other metrics by AGS
    agg_areas_df = area_gdf[["AGS","KFL","EWZ"]       # select only relevant cols
                        ].groupby("AGS").sum() 
    
    # select correct shapes, and dissolve them by AGS/GEN
    exists_gdf = area_gdf[(area_gdf.KFL>0)][["AGS","GEN","geometry"]].dissolve(by="AGS")
    

    # join together valid shapes, aggregate numbers, and stop counts
    agg_gdf = exists_gdf.merge(agg_counts_df, on='AGS'
                        ).merge(agg_areas_df, on='AGS')
    # Merge SFL onto shape
    # reading in the corresponding Siedlungsflaeche
    area_sfl_path = sfl_tables[level]
    area_sfl_df = pd.read_csv(area_sfl_path, skiprows = [1], sep = ";", decimal = ",")
    
    # Add in Siednlungsflächenanteil
    agg_sfl_gdf = agg_gdf.merge(area_sfl_df[["Kennziffer",'Raumeinheit','Anteil Siedlungs- und Verkehrsfläche']], how="left", left_on = "AGS", right_on = "Kennziffer")
    
    # calculate proper SFL, other numbers
    agg_sfl_gdf['SFL'] = agg_sfl_gdf['KFL'] * agg_sfl_gdf['Anteil Siedlungs- und Verkehrsfläche']/100
    for scope in pointfiles.keys():
        agg_sfl_gdf['n.'+scope+'.ewz'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['EWZ']
        agg_sfl_gdf['n.'+scope+'.kfl'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['KFL']
        agg_sfl_gdf['n.'+scope+'.sfl'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['SFL']
    # what I actually need: individual AGS, Raumeinheit, sum EWZ, KFL, SFL, n, selected geometry 
    agg_sfl_gdf[['AGS', 'Raumeinheit', 'EWZ', 'KFL','SFL']+[col for col in agg_sfl_gdf.columns if col.startswith('n.')] + ['geometry']
    ].to_file(out_path + level +".stops.3857.geojson",driver="GeoJSON")
    print("Wrote " + level +".stops.3857.geojson")



###############
    # grid
###############
    
# make a grid -- one for each sidelength, in good projection
# Here, all grids will be in EPSG3035, a Germany-centered EA-projection
from shapely.geometry import Polygon
import numpy as np

# Germany bounding box
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).to_crs('epsg:3035')
germany = world[world.name == "Germany"] # we could do a grid around all points, 
                                        # but that seems a bit excessive right now; it might be quicker this way
# Convex hull bounding box                  
ncounts_df = pd.read_csv(pointfiles['all'])
ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
                               geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
                               crs="epsg:4326").to_crs("epsg:3035")
stopspace = gpd.GeoDataFrame({'geometry':[ncounts_gdf.unary_union.convex_hull]}, crs="epsg:3035")
        

# iterate little boxes with sidelength sl:                                       
xmin,ymin,xmax,ymax =  stopspace.total_bounds

sls = [5, 1] # km 50, 10, 1, 
#grids = {}
for scale in sls:
    print("Making grid with sidelength "+ str(scale) + "km")
    slm = scale*1000 # m
    rows = int(np.ceil((ymax-ymin) /  slm)) 
    cols = int(np.ceil((xmax-xmin) / slm))  
    print(str(rows) + " x " + str(cols))
    XleftOrigin = xmin
    XrightOrigin = xmin + slm
    YtopOrigin = ymax
    YbottomOrigin = ymax- slm
    polygons = []
    for i in range(cols):
        Ytop = YtopOrigin
        Ybottom =YbottomOrigin
        for j in range(rows):
            polygons.append(Polygon([(XleftOrigin, Ytop), (XrightOrigin, Ytop), (XrightOrigin, Ybottom), (XleftOrigin, Ybottom)])) 
            Ytop = Ytop - slm
            Ybottom = Ybottom - slm
        XleftOrigin = XleftOrigin + slm
        XrightOrigin = XrightOrigin + slm
    grid = gpd.GeoDataFrame({'geometry':polygons})
    grid.crs = 'epsg:3035'
    grid = grid[~gpd.sjoin(grid, stopspace, how='left', op='intersects')["index_right"].isna()] # choose squares that overlap with convex hull
#    grids[scale] = grid                                                             # either save all the grids in a dict...
    grid.to_file(out_path + str(scale) +"k.grid.3035.geojson",driver="GeoJSON")     # or save them to file--if it gets big that may be better
#    ax = grid.boundary.plot()
#    germany.boundary.plot(ax=ax)
    

# repeat the above exercise with the grid
# get counts of stops in all shapes associated with each AGS
for scale in sls:
    print("Getting counts for grid with sidelength "+ str(scale) + "km")
#    agg_counts_gdf = grids[scale] # or if this gets too unwieldy load from disk...
    agg_counts_gdf = gpd.read_file(out_path + str(scale) +"k.grid.3035.geojson",driver="GeoJSON")
    ### !!! I'm worried we might lose shapes here, if they don't have one of the type of counts. Better to keep them separate (like above)
    for scope in pointfiles.keys():
        print("... counting "+scope+"-stops ...")
        # read counts per station, with point locations
        ncounts_df = pd.read_csv(pointfiles[scope])
        ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
                                       geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
                                       crs="epsg:4326").to_crs("epsg:3035")
        ## get sum of stuff in each AGS
        agg_counts_gdf = gpd.sjoin(agg_counts_gdf, ncounts_gdf[["n","geometry"]], how="left", op="contains" # spatial join
                            ).drop("index_right",axis=1).reset_index().dissolve(by="index",aggfunc='sum').rename({"n":"n."+scope},axis=1)
    agg_counts_gdf = agg_counts_gdf[agg_counts_gdf['n.all']>0]
    agg_counts_gdf.to_crs('epsg:4326', inplace=True)
    print("Writing "+ str(scale) +"k.stops.4326.geojson")
    agg_counts_gdf.to_file(out_path + str(scale) +"k.gesamtnetz.stops.4326.geojson",driver="GeoJSON")

