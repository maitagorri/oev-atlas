#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 20:51:56 2021

@author: maita
"""

import fiona
import pandas as pd
import geopandas as gpd

# load the grid layers
gridfiles = {"50k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/50k.3857.geojson",
        "5k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/5k.3857.geojson",
        "0.5k":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/0.5k.3857.geojson"}

# load the points layers
pointfiles = {"fv":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/fv.stops.csv",
          "rs":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/rs.stops.csv",
          "nv":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/nv.stops.csv"}

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
