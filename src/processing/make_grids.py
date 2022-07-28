# make a grid -- one for each sidelength, in good projection
# Here, all grids will be in EPSG3035, a Germany-centered EA-projection
from shapely.geometry import Polygon
import numpy as np
import geopandas as gpd

# define paths
workingdir = "../../data/interim/" 

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

# Make grids
grids = {s:make_grid(s,germany) for s in sls}

# Write grids
for s in sls:
    grids[s].to_file(workingdir + '{}k.geojson'.format(s), driver="GeoJSON")
    