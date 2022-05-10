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

# Welcher Datensatz?
zipname = '20220419_fahrplaene_gesamtdeutschland_gtfs' # name of GTFS zipfile


# Welche Pfade?
out_dir = "../../data/processed/"
work_dir = "../../data/interim/"
raw_dir = "../../data/raw/"

# define the points layers--these will all be included in the output, with this dictionary's keys identifying columns

pointfiles = {
              "fz": work_dir + zipname + ".fz.nstops.csv",
              "fb": work_dir + zipname + ".fb.nstops.csv",
              "nv": work_dir + zipname + ".nv.nstops.csv"
             }
# check that files are present

for file in pointfiles.values():
    try: 
        open(file)
    except:
        raise(FileNotFoundError(file + " missing"))

# dataset_name = "nah-fern-211015" # filename for output files


###############
    # areas
###############

# define the area layers
# make sure your zip-files' structure agrees with this
admin_area_file = raw_dir + 'bkg/' + 'vg250-ew_12-31.utm32s.shape.ebenen.zip'
shapefile_names = {"gem":"VG250_GEM.shp",
                  "krs":"VG250_KRS.shp",
                  "lan":"VG250_LAN.shp"}

# define additional information tables
# these will depend on the names under which you saved your INKAR export
sfl_tables= {"gem":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Gemeinde.csv",
              "krs":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Kreis.csv",
              "lan":raw_dir + 'inkar/' + "Tabelle_Siedlungsflaeche_Land.csv"}


def scopeCountsInAreas(colname, ncounts_df, area_gdf):
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
                        ].groupby("AGS").sum().rename({"n_day":colname},axis=1
                        ) # keep AGS that have counts in them
    return(agg_counts_df)
    

    
def loadAreas(area_path):
    ## clean the area file for our purposes
    area_gdf = gpd.read_file(area_path)
    # to compare to other sources, get reasonable type for AGS
    area_gdf.AGS = area_gdf.AGS.astype(int) 
    # fix projection, aggregate qualitative
    area_qual_gdf = area_gdf.to_crs("epsg:4326"
                        )[(area_gdf.KFL>0)
                        ][["AGS","GEN","SN_L","SN_K","SN_G","geometry"] # select correct shapes
                        ].dissolve(by="AGS")                     # and dissolve them by AGS/GEN
    # aggregate quantitative
    area_quant_df = area_gdf[["AGS","KFL","EWZ"]
                             ].groupby('AGS'
                             ).sum(
                             ).reset_index()
    
    # put it all together
    area_gdf = area_qual_gdf.merge(area_quant_df, how='left', on='AGS')
    return(area_gdf)
    
    
def mergeAreasInkar(gdf, sfl_paths, level):
    ## Merge INKAR data onto shapes
    # reading in the corresponding Siedlungsflaeche
    area_sfl_path = sfl_paths[level]
    area_sfl_df = pd.read_csv(area_sfl_path, skiprows = [1], sep = ";", decimal = ",")
    
    # Add in Siednlungsflächenanteil
    sfl_gdf = gdf.merge(area_sfl_df[["Kennziffer",'Anteil Siedlungs- und Verkehrsfläche']],
                                how="left", 
                                left_on = "AGS", 
                                right_on = "Kennziffer")
    # calc actual SFL
    sfl_gdf['SFL'] = sfl_gdf['KFL'] * sfl_gdf['Anteil Siedlungs- und Verkehrsfläche']/100
    
    # drop unnecessary cols
    sfl_gdf = sfl_gdf.drop(['Kennziffer','Anteil Siedlungs- und Verkehrsfläche'], axis=1)
    
    return(sfl_gdf)
    

def addScopeCounts(gdf, pointfiles):
    ## add counts to shapes
    # get counts of stops in all shapes associated with each AGS
    df = gdf
    for scope in pointfiles.keys():        
        print("Counting "+scope)
        ncounts_df = pd.read_csv(pointfiles[scope])
        df = scopeCountsInAreas("halte."+scope, 
                                           ncounts_df, 
                                           gdf
                                          ).merge(df, how="right", on="AGS")
    # restore spatial df
    gdf = gpd.GeoDataFrame(df, geometry=df.geometry)
    return(gdf)




def tidyCountsGdf(gdf, scopes):
    ## Finalize data content
    # calculate totals
    gdf['halte.ges'] = gdf[['halte.'+scope for scope in scopes]].sum(axis=1)
    # calculate ratios
    for scope in ['ges'] + scopes:
        gdf['halte.'+scope+'.EWZ'] = gdf['halte.'+scope]/gdf['EWZ']
        gdf['halte.'+scope+'.SFL'] = gdf['halte.'+scope]/gdf['SFL']
        
    ## Tidying up
    gdf.rename(columns={"GEN":level.upper()}, inplace=True)
    # what I actually need: individual AGS, Raumeinheit, sum EWZ, KFL, SFL, n, selected geometry 
    return(gdf[[level.upper(),"SN_L","SN_K", "SN_G", 'EWZ', 'KFL','SFL'] +
                            ['halte.'+scope for scope in ['ges'] + scopes] + 
                            ['halte.'+scope +'.'+q for scope in ['ges'] + scopes for q in ['EWZ','SFL']] +
                            ['geometry']])

def aggregateShapes(gdf, pointfiles, level):
    gdf = mergeAreasInkar(gdf, sfl_tables, level)
    gdf = addScopeCounts(gdf, pointfiles)
    scopes = list(pointfiles.keys())
    gdf = tidyCountsGdf(gdf,scopes)
    return(gdf)

# extract zip files
zf = zipfile.ZipFile(admin_area_file)
zf.extractall(work_dir)

### run over all levels
for level in shapefile_names.keys():
    shapefile_name = shapefile_names[level]
    # Read in admin areas
    area_gdf = loadAreas(work_dir + 'vg250-ew_12-31.utm32s.shape.ebenen/vg250-ew_ebenen_1231/' + shapefile_names[level])
    # process
    agg_gdf = aggregateShapes(area_gdf, pointfiles, level)
    # write out:
    out_file = zipname + "." + level.upper() +".geojson"
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
    out_file = zipname + "_" + str(scale) +"k.geojson"
    print("Writing "+ out_file)
    countgrids[scale].to_file(out_dir + out_file,driver="GeoJSON")

