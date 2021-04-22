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
                              
                                        
xmin,ymin,xmax,ymax =  germany.total_bounds

sls = [50, 10, 5, 1] # km
grids = {}
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
    grids[scale] = grid
    ax = grid.boundary.plot()
    germany.boundary.plot(ax=ax)
    
# repeat the above exercise with the grid
# get counts of stops in all shapes associated with each AGS
for scale in sls:
    print("Getting counts for grid with sidelength "+ str(scale) + "km")
    agg_counts_gdf = grids[scale]
    for scope in pointfiles.keys():
        print("... counting "+scope"-stops ...")
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
    print("Writing "+ str(scale) +"k.stops.3857.geojson")
    agg_counts_gdf.to_file(out_path + str(scale) +"k.stops.3857.geojson",driver="GeoJSON")


# load the grid layers
gridfiles = {"50k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/50k.3857.geojson",
        "5k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/5k.3857.geojson",
        "0.5k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/0.5k.3857.geojson"}

def read_process_points(fname):
    #    fname = pointfiles["fv"]
    # specific read and process function for this set of points
    points_agg = pd.read_csv(fname, usecols = [0,2,3]
        # group, and count stops with same lat/lon
        ).groupby(['stop_lat', 'stop_lon']).count().rename({"stop_name":"n"},axis=1).reset_index()
    points_geo = gpd.GeoDataFrame(points_agg, geometry = gpd.points_from_xy(points_agg.stop_lon, points_agg.stop_lat), crs="epsg:4326").to_crs("epsg:3857")
    return points_geo


def npip(grid, points):
    # find for each point which polygon it is in
    ## assign id to each polygon
    grid["grid_id"] = [i for i in range(len(grid))]
    ## look only at points in grid area
    xmin, ymin, xmax, ymax = grid.total_bounds
    points_inside = points.cx[xmin:xmax, ymin:ymax]
    
    ## find which polygon each point belongs to
    pip = gpd.sjoin(points_inside, grid, op="within")
    # now count up occurrence of each grid polygon
    n = pip.groupby('grid_id').sum().n.reset_index()#pd.value_counts(pip.id)
    return n


gridname = "50k"
pointname = "fv"

# count points and write them out
for gridname in gridfiles.keys():
    grid = gpd.read_file(gridfiles[gridname])
    for pointname in pointfiles.keys():
        points = read_process_points(pointfiles[pointname])
        print((gridname, pointname))
        npip(grid, points).to_csv("/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"+gridname + ".3857." + pointname + ".csv", index=False)

for gridname in gridfiles.keys():
    grid = gpd.read_file(gridfiles[gridname])
    for pointname in pointfiles.keys():
        print((gridname, pointname))
        counts= pd.read_csv("/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"+gridname + ".3857." + pointname + ".csv").set_index("grid_id")
        grid=grid.join(counts).rename({"n":"n."+pointname},axis=1)
    grid["n"] = grid["n.nv"].fillna(0)+grid["n.rs"].fillna(0)+grid["n.nv"].fillna(0)
    grid_sparse = grid[grid.n>0]
    grid_sparse.to_file("/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"+gridname+".3857.stops.geojson",driver="GeoJSON")
    
grid = gpd.read_file("/home/maita/Nextcloud/Documents/Work/Gap_Map/out/0.5k3857.stops.geojson")

grid.columns
grid[~grid["n.nv"].isna()][["n.fv","n.rs","n.nv","n"]].head()
