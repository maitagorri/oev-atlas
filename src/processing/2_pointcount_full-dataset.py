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
import zipfile
import re

# Welche Pfade?
out_dir = "../../data/processed/"
work_dir = "../../data/interim/"
raw_dir = "../../data/raw/"

# define the points layers--these will all be included in the output, with this dictionary's keys identifying columns

pointfiles = {"nv": work_dir + "20211015_fahrplaene_gesamtdeutschland_gtfs.nstops.csv",

              "fv": work_dir + "2021_reissue_2.fv.nstops.csv"
             }
# check that files are present

for file in pointfiles.values():
    try: 
        open(file)
    except:
        raise(FileNotFoundError(file + " missing"))

dataset_name = "nah-fern-211015" # filename for output files


###############
    # areas
###############

# define the area layers
# make sure your zip-files' structure agrees with this
admin_area_file = raw_dir + 'bkg/' + 'vg250-ew_12-31.utm32s.shape.ebenen.zip'
shapefile_names = {"gem":"VG250_GEM.shp",
                  "kre":"VG250_KRS.shp",
                  "lan":"VG250_LAN.shp"}

# define additional information tables
# these will depend on the names under which you saved your INKAR export
sfl_tables= {"gem":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Gemeinde.csv",
              "kre":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Kreis.csv",
              "lan":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Land.csv"}


def scopeCountsInAreas(countname, ncounts_df, area_gdf):
    # read counts per station, with point locations
    ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
                                   geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
                                   crs="epsg:4326").to_crs("epsg:4326")
    ## get sum of stuff in each AGS
    agg_counts_df = gpd.sjoin(area_gdf[["AGS","geometry"]], 
                              ncounts_gdf[["n_day","geometry"]], 
                              how="left", 
                              op="contains" # spatial join
                        )[["AGS","n_day"]
                        ].groupby("AGS").sum().rename({"n_day":countname},axis=1
                        ) # keep AGS that have counts in them
    return(agg_counts_df)
    
    
def aggregateShapes(area_gdf, pointfiles):
    # fix projection
    area_gdf = area_gdf.to_crs("epsg:4326")
    area_gdf.AGS = area_gdf.AGS.astype(int) # to compare to other sources, get reasonable type for AGS
  
    # get counts of stops in all shapes associated with each AGS
    agg_counts_df = area_gdf[["AGS"]].drop_duplicates()
    for scope in pointfiles.keys():        
        print("Counting "+scope)
        ncounts_df = pd.read_csv(pointfiles[scope])
        agg_counts_df = scopeCountsInAreas("n."+scope, 
                                           ncounts_df, 
                                           area_gdf
                                          ).merge(agg_counts_df, how="right", on="AGS") 

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
    agg_sfl_gdf = agg_gdf.merge(area_sfl_df[["Kennziffer",'Raumeinheit','Anteil Siedlungs- und Verkehrsfläche']],
                                how="left", 
                                left_on = "AGS", 
                                right_on = "Kennziffer")
    
    # calculate proper SFL, other numbers
    agg_sfl_gdf['SFL'] = agg_sfl_gdf['KFL'] * agg_sfl_gdf['Anteil Siedlungs- und Verkehrsfläche']/100
    for scope in pointfiles.keys():
        agg_sfl_gdf['n.'+scope+'.ewz'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['EWZ']
        agg_sfl_gdf['n.'+scope+'.kfl'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['KFL']
        agg_sfl_gdf['n.'+scope+'.sfl'] = agg_sfl_gdf['n.'+scope]/agg_sfl_gdf['SFL']
    # add across all scopes
    agg_sfl_gdf['n.ges.sfl'] = agg_sfl_gdf[['n.'+scope+'.sfl' for scope in pointfiles.keys()]].sum(axis=1)
    # what I actually need: individual AGS, Raumeinheit, sum EWZ, KFL, SFL, n, selected geometry 
    return(agg_sfl_gdf[['AGS', 'Raumeinheit', 'EWZ', 'KFL','SFL']+[col for col in agg_sfl_gdf.columns if col.startswith('n.')] + ['geometry']])


# extract zip files
zf = zipfile.ZipFile(admin_area_file)
zf.extractall(work_dir)

for level in shapefile_names.keys():
    shapefile_name = shapefile_names[level]
    # Read in admin areas
    area_gdf = gpd.read_file(work_dir + 'vg250-ew_12-31.utm32s.shape.ebenen/vg250-ew_ebenen_1231/' + shapefile_name)
    # process
    agg_gdf = aggregateShapes(area_gdf, pointfiles)
    # write out:
    out_file = dataset_name + "_" + level +".stops.4326.geojson"
    agg_gdf.to_file(out_dir + out_file, driver="GeoJSON")
    print("Wrote " + out_file)



###############
    # grid
###############
    
# make a grid -- one for each sidelength, in good projection
# Here, all grids will be in EPSG3035, a Germany-centered EA-projection
from shapely.geometry import Polygon
import numpy as np

# Germany bounding box
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).to_crs('epsg:3035')
germany = world[world.name == "Germany"] 
germany['geometry'] = germany.geometry.buffer(50000)#50k buffer
germany = germany.to_crs('epsg:4326') # we are in 43

# Convex hull bounding box __around Germany__              

stopspace = gpd.GeoDataFrame({'geometry':[germany.unary_union.convex_hull]}, crs="epsg:4326")

sls = [5, 1] # sidelengths of desired grids in kilometers

def make_grid(scale, stopspace):   
    # scale is a sidelength in km
    # stopspace is a Geodataframe containing the complex hull of the space to be covered with grid, IN EPSG:3035
    print("Making grid with sidelength "+ str(scale) + "km")
    stopspace = stopspace.to_crs("epsg:3035") # in 3035 for grid making
    # iterate little boxes with sidelength sl:
    bounds = stopspace.total_bounds
    xmin,ymin,xmax,ymax =  bounds
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
    grid = grid[~gpd.sjoin(grid, stopspace, how='left', op='intersects')["index_right"].isna()] 
    return(grid.to_crs("epsg:4326"))

def aggregateGrid(grid_gdf, ncounts_gdf, scope):
    print("Getting " + scope + " counts for grid with " + str(len(grid_gdf)) + " polygons")
    
    agg_counts_gdf = grid_gdf.to_crs('epsg:4326')
    ## get sum of stuff in each AGS
    agg_counts_gdf = gpd.sjoin(agg_counts_gdf, 
                               ncounts_gdf[["n_day","geometry"]].to_crs('epsg:4326'), 
                               how="left", 
                               op="contains" # spatial join
                       ).drop("index_right",axis=1
                       ).reset_index(
                       ).dissolve(by="index",aggfunc='sum'
                       ).rename({"n_day":"n."+scope},axis=1)
    agg_counts_gdf = agg_counts_gdf[agg_counts_gdf['n.'+scope]>0].drop(columns='geometry')
        # reset index to geometry for easier joining of different counts across grid later
    return(agg_counts_gdf)
    

# Read in point files (as dictionary)
def pointfileReader(filepath, stopspace):
    points = pd.read_csv(filepath)
    points_gdf = gpd.GeoDataFrame(points,
                                  geometry = gpd.points_from_xy(points.stop_lon, 
                                                                points.stop_lat),
                                  crs="epsg:4326").to_crs("epsg:4326")
    points_gdf = gpd.sjoin(points_gdf, stopspace[['geometry']], how="inner", op="within" # spatial join
                        )
    return(points_gdf)
    
# Make grids
grids = {s:make_grid(s,germany) for s in sls}
# get pointfiles ready
points = {k:pointfileReader(pointfiles[k], germany) for k in pointfiles}


# For each scale, aggregate all the different count files across grid, then concatenate
countgrids = { 
    scale:pd.concat(                  # concatenating the different scope counts of one grid
        [aggregateGrid(grids[scale],  # aggregating given scope counts on grid
                       points[scope], 
                       scope) 
         for scope in pointfiles      # for all scopes/different input count files
        ],
        axis=1
    ).join(grids[scale]               # getting the geometry column back 
    )
    for scale in sls                  # for all different grid scales
}

# Add total count to each grid
for scale in countgrids:
    countgrids[scale]['n.ges'] = countgrids[scale][['n.'+scope for scope in pointfiles]].sum(axis=1)

# check count consistency
for scope in pointfiles:
    n = points[scope].n_day.sum()
    gridn = [countgrids[scale]['n.'+scope].sum() for scale in sls]
    if any([g!=n for g in gridn]):
        print('Problem mit ' + scope + '!!')
        print(n)
        print(gridn)
        
# check average counts        
for scope in pointfiles:
    print ("Durchschnittliche " + scope + '-Abfahrten je Haltestelle und Tag:')
    print (points[scope].n_day.sum()/points[scope].shape[0])

    # write out:
for scale in sls:
    out_file = dataset_name + "_" + str(scale) +"k.stops.4326.geojson"
    print("Writing "+ out_file)
    countgrids[scale].to_file(out_dir + out_file,driver="GeoJSON")

