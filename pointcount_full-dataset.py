#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 20:51:56 2021

@author: maita
"""


import pandas as pd
import geopandas as gpd

# define the points layers
out_path = "/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"
pointfiles = {"fv":out_path + "fv.nstops.csv",
          "all":out_path + "nstops.csv"}

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

for scope in pointfiles.keys():
    for level in shapefiles.keys():
        # what about the admin areas?
        gem_path = shapefiles[level]
        gem_gdf = gpd.read_file(gem_path)
        
        #gem_gdf.head()
        #gem_gdf.crs
        gem_gdf = gem_gdf.to_crs("epsg:3857")
        #gem_gdf.columns
        gem_gdf.reset_index(inplace=True) #index to identify individual shapes
        
        # reading in also the corresponding Siedlungsflaeche
        gem_sfl_path = sfl_tables[level]
        gem_sfl_df = pd.read_csv(gem_sfl_path, skiprows = [1], sep = ";", decimal = ",")
        #gem_sfl_df.head()
        
        # try how projections of the points fit into this
        ncounts_df = pd.read_csv(pointfiles[scope])
        #ncounts_df.head()
        ncounts_gdf = gpd.GeoDataFrame(ncounts_df,
                                       geometry = gpd.points_from_xy(ncounts_df.stop_lon, ncounts_df.stop_lat),
                                       crs="epsg:4326").to_crs("epsg:3857")
        
        # Merge SFL onto shape
            ## checking what field makes most sense to use for merge
        #[s for s in gem_gdf.AGS_0.astype('int64') if s in gem_sfl_df.Kennziffer]
        
        #sum(gem_sfl_df.Kennziffer.isin(gem_gdf.AGS_0.astype(int)))
        #sum(gem_sfl_df.Kennziffer.isin(gem_gdf.AGS.astype(int)))
        
        #gem_sfl_df[~gem_sfl_df.Kennziffer.isin(gem_gdf.AGS_0.astype(int))][["Raumeinheit","Kennziffer"]]
        #gem_gdf[~gem_gdf.AGS_0.astype(int).isin(gem_sfl_df.Kennziffer)][["GEN","AGS_0"]]
            ## es scheint als ob einige Kennziffern sich nicht 100% decken
        
        gem_gdf.AGS = gem_gdf.AGS.astype(int)
        gem_sfl_gdf = gem_gdf.merge(gem_sfl_df, how="left", left_on = "AGS", right_on = "Kennziffer")
        # !!! actually do this at the very end
        
        
        
        # Merge shapes with pointcounts
        sum_gdf = gpd.sjoin(gem_sfl_gdf, ncounts_gdf, how="left", op="contains" # spatial join
                            )[["index", "n"]                                    # select only relevant cols
                            ].groupby("index").sum()                            # groupby shapes and sum
        agg_df = gem_sfl_gdf.merge(sum_gdf, how="left", on="index")            # join shapes & info back on
        agg_df["SFL_frac"] = agg_df["Anteil Siedlungs- und Verkehrsfläche"]/100 # clean up SFL
        
        clean_df = agg_df[["AGS","GEN","EWZ", "KFL", "SFL_frac","n","geometry"]]
            # select relevant cols
        # clean_df.to_file(out_path + level +"."+scope+".stops.3857.geojson",driver="GeoJSON")
        agg_df.to_file(out_path + level +"."+scope+".stops.3857.geojson",driver="GeoJSON")



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
