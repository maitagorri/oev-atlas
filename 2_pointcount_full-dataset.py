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

# Welche Pfade?
out_dir = "/home/jupyter-maita.schade/VW_Data_Hub/Gap_Map/out/"
# out_dir = "/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"
geo_path = "/home/jupyter-maita.schade/VW_Data_Hub/Gap_Map/raw/geo/"
# geo_path = "/home/maita/VW_Data_Hub/Gap_Map/raw/geo/"

# define the points layers
# out_path = out_dir + jahr + "/"
zipname = "delfi-brosi-2021"
# zipname = sys.argv[2]
pointfiles = {"nv": "/home/jupyter-maita.schade/VW_Data_Hub/Gap_Map/out/delfi/20211015_fahrplaene_gesamtdeutschland_gtfs.nstops.csv",
              # out_dir + "Delfi/20211105_fahrplaene_gesamtdeutschland_gtfs.nstops.csv",

              "fv": "/home/jupyter-maita.schade/VW_Data_Hub/Gap_Map/out/2021/2021_reissue_2fv.nstops.csv"
              #out_dir + "Brosi/2021/2021_reissue_2fv.nstops.csv"
             }

###############
    # areas
###############

# define the area layers
admin_area_path = geo_path + "vg250-ew_12-31.utm32s.shape.ebenen/vg250-ew_ebenen_1231/"
shapefiles = {"gem":admin_area_path + "VG250_GEM.shp",
              "kre":admin_area_path + "VG250_KRS.shp",
              "lan":admin_area_path + "VG250_LAN.shp"}

# define additional information tables
sfl_tables= {"gem":geo_path + "Tabelle_Siedlungsflaeche_Gemeinde.csv",
              "kre":geo_path + "Tabelle_Siedlungsflaeche_Kreis.csv",
              "lan":geo_path + "Tabelle_Siedlungsflaeche_Land.csv"}


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
    # what I actually need: individual AGS, Raumeinheit, sum EWZ, KFL, SFL, n, selected geometry 
    return(agg_sfl_gdf[['AGS', 'Raumeinheit', 'EWZ', 'KFL','SFL']+[col for col in agg_sfl_gdf.columns if col.startswith('n.')] + ['geometry']])


for level in shapefiles.keys():
#     level = "lan"
    area_path = shapefiles[level]
    # Read in admin areas
    area_gdf = gpd.read_file(area_path)
    # process
    agg_gdf = aggregateShapes(area_gdf, pointfiles)
    # write out:
    out_file = zipname + "_" + level +".stops.4326.geojson"
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

# we could do a grid around all points, 
# but that seems a bit excessive right now; it might be quicker this way
# ncounts_df = pd.read_csv(pointfiles['all'])
# ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
#                                geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
#                                crs="epsg:4326").to_crs("epsg:3035")


# iterate little boxes with sidelength sl:                                       
# xmin,ymin,xmax,ymax =  stopspace.total_bounds
sls = [5, 1] # km 50, 10, 1, 

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
#    agg_counts_gdf = grids[scale] # or if this gets too unwieldy load from disk...
    agg_counts_gdf = grid_gdf.to_crs('epsg:4326')
    ### !!! I'm worried we might lose shapes here, if they don't have one of the type of counts. Maybe better to keep them separate (like above)
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
    out_file = zipname + "_" + str(scale) +"k.stops.4326.geojson"
    print("Writing "+ out_file)
    countgrids[scale].to_file(out_dir + out_file,driver="GeoJSON")

