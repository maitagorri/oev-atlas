#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 19 14:06:32 2021

@author: maita

Checking differences between years in admin areas
"""

import pandas as pd
import geopandas as gpd
import numpy as np

datadir = "/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"
scopes = ["all","fv"]
levels = ["gem", "kre","lan"]
years = ["2020","2021"]

dirs = {y:datadir+y+"/" for y in years}
gdfs = {y:{
            level:gpd.read_file(dirpath+level+".stops.4326.geojson") for level in levels
            } for (y,dirpath) in dirs.items()}

{level:gdfs for level in levels}

gem_20_gdf= gdfs["2020"]["gem"]
gem_21_gdf= gdfs["2021"]["gem"]
gem_21_gdf.columns
gem_20_gdf.merge(gem_21_gdf.drop("geometry", axis=1), how="right", on="AGS")


def ndiff(df1, df2, idcols, func = lambda a,b: b-a):
    func = np.vectorize(func)
    ncols = [col for col in df1.columns if (not (col in idcols) and col in df2.columns and df1[col].dtype=="float64")]
    merge_df = df1.merge(df2.drop(["geometry"], axis=1), on=idcols)
    diff_df = pd.DataFrame({col+"_diff":func(merge_df[col+"_x"],merge_df[col+"_y"]) for col in ncols})
    diff_df[idcols] = merge_df[idcols]
    diff_gdf = df1.drop(ncols, axis=1).merge(diff_df, on=idcols)
    return(diff_gdf)
    
#diff_gdf = ndiff(gem_20_gdf,gem_21_gdf,"AGS")
diff_gdf = ndiff(gem_20_gdf,gem_21_gdf,"AGS", func = lambda a,b: (b-a)/a if a>0 else 0 if b-a==0 else None)
diff_gdf.plot("n.all_diff", figsize = (10,10), legend = True)
diff_gdf["n.all_diff"].hist(range=(-100,100))
np.mean(diff_gdf["n.all_diff"])
np.std(diff_gdf["n.all_diff"])
np.median(diff_gdf["n.all_diff"])
diff_gdf["n.all_diff"].describe()

diff_gdf.merge(gem_20_gdf, on="AGS").loc[np.abs(diff_gdf["n.all_diff"])>10,["Raumeinheit", "n.all","n.all_diff"]]

(df1["n.all"]-df2["n.all"])/df1['n.all']

f = np.vectorize(lambda a,b: (b-a)/a if a > 0 else np.nan)
