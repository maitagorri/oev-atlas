#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 23:22:49 2021

@author: maita
"""
# load libraries
import pandas as pd
import os, re

# define paths
rawdir = "/home/maita/Nextcloud/Documents/Work/Gap_Map/raw/"
rawdatadir = rawdir + "GTFS/2021/"
outdir = "/home/maita/Nextcloud/Documents/Work/Gap_Map/out/"


def getRouteShortNames(scope):
    # Relying on pre-separated routes file in raw directory
    # takes scope prefix and gets short_names to filter for
    print("Scope for routes: "+ scope)
    routespath = [s for s in os.listdir(rawdir) if re.search("routes_"+scope, s) ][0]
    routes_df = pd.read_csv(rawdir + routespath)
    routenames = routes_df.route_short_name.unique()
#    routeids = routes_df.route_id.unique()
    return(routenames)

def filterByRoute(stop_times_df, routenames, routes_path = rawdatadir + "routes.txt", trips_path = rawdatadir + "trips.txt"):
    # Given a list of route_short_names included in scope
    # relying on or taking routes and trips in rawdatadir
    # takes stop_times and filters them to include only stops made on routes included in scope
    print("Filtering routes...")
    routes_df = pd.read_csv(routes_path, usecols = ["route_short_name", "route_id"])
    trips_df = pd.read_csv(trips_path, usecols = ['route_id', "trip_id"])
    stop_times_filtered = stop_times_df.merge(
            trips_df.merge(
                routes_df[routes_df["route_short_name"].isin(routenames)][["route_id"]], # which routes are ok?
                how="right")[["trip_id"]], # which trips are on those routes?
                how="right" # which stops were made on those trips?
                )
    print("Total stops: ", len(stop_times_filtered))
    return(stop_times_filtered)

def countPerStop(stop_times_df):
    # collapses DataFrame of stop_times to counts per stop_id
    print("Counting stops")
    return(stop_times_df.groupby('stop_id').count().rename({"trip_id":"n"},axis=1).reset_index())
 
def addLocationsToStops(counts_df, stops_path = rawdatadir + "stops.txt"):
    # Relying on or given path to file with stops and locations
    # takes stop_time-counts DataFrame and enriches them with locations
    # returns DataFrame of stops with stop information and counts
    print("Adding locations to stops")
    stops_df = pd.read_csv(stops_path)
    return(stops_df.merge(counts_df, how="right", on = "stop_id"))
    
def readStopTimes(rawdatadir):
    # helper function to read in necessary columns of stop_times file in rawdatadir
    print("Loading " + rawdatadir)
    return(pd.read_csv(rawdatadir + "stop_times.txt", usecols = ["stop_id","trip_id"]))
    
# Count and write out all stops per location
addLocationsToStops(
        countPerStop(
                readStopTimes(rawdatadir)
                )
        ).to_csv(outdir + "nstops.csv")

# Count and write out only FV-stops per location
addLocationsToStops(
        countPerStop(
                filterByRoute(
                        readStopTimes(rawdatadir), 
                        getRouteShortNames("fv"))
                )
        ).to_csv(outdir + "fv.nstops.csv")

#outfiles = {"fv":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/fv.nstops.csv",
#          "rs":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/rs.nstops.csv",
#          "nv":"/home/maita/Nextcloud/Documents/Work/Gap_Map/out/nv.nstops.csv"}
#
## figure out how to distinguish routes
#
#scopes = outfiles.keys()
#routes = [pd.read_csv(rawdir+"Archiv/"+scope+"/routes.txt") for scope in scopes]
#agencies = [r.agency_id.unique() for r in routes]
#agencies[0]
#
## function to get applicable routes
#scope = "fv"
#
#
#def getRouteNames(scope, moniker):
#    routespath = [s for s in os.listdir(rawdir) if re.search("routes_"+scope, s) ][0]
#    routes_df = pd.read_csv(rawdir + routespath)
#    routenames = routes_df[moniker].unique()
##    routeids = routes_df.route_id.unique()
#    return(routenames)
#
#getRouteNames("fv", "route_long_name")
#    
##fv_ids = getRouteShortNames("fv")[0]
##nv_ids = getRouteShortNames("nv")[0]
##
##[i for i in fv_ids if i in nv_ids]
### note that ids in the split-up dataset are fungible--shortnames are not
#  
## function to filter stop_times in scope
#
#def filterTripsInScope (scope, routes, trips, stop_times, moniker):
#    # function to filter a df with stop_id and trip_id, given dfs of trips and routes,
#    # for only those trips contained in a given scope
#    print(scope)
#    routenames = getRouteNames(scope, moniker)
#    print(routenames)
#    
#    stop_times_scope = stop_times.merge(
#            trips.merge(
#                    routes[routes[moniker].isin(routenames)][["route_id"]], # which routes are ok?
#                    how="right")[["trip_id"]], # which trips are on those routes?
#                    how="right" # which stops were made on those trips?
#                    )
#    return(stop_times_scope)
#
#
#    
#            
#allroutes = pd.read_csv(rawdatadir + "routes.txt", usecols = ["route_short_name", "route_long_name", "route_id"])
#alltrips = pd.read_csv(rawdatadir + "trips.txt", usecols = ['route_id', "trip_id"])
#allstop_times = pd.read_csv(rawdatadir + "stop_times.txt", usecols = ["stop_id","trip_id"])  
#
#n = []
#for scope in scopes:
#    n.append(len(filterTripsInScope(scope, allroutes, alltrips, allstop_times, "route_long_name")))    
#sum(n)
#len(allstop_times)
# oh nein die summe ist groesser als der ursprung
#
#routenames = {}
#for scope in scopes:
#    routenames[scope] = getRouteNames(scope, "route_long_name")
#
#[n for n in routenames["nv"] if n in routenames["rs"]]
#[n for n in routenames["nv"] if n in routenames["fv"]]
#[n for n in routenames["rs"] if n in routenames["nv"]]
#[n for n in routenames["rs"] if n in routenames["fv"]]
#[n for n in routenames["fv"] if n in routenames["nv"]]
#[n for n in routenames["fv"] if n in routenames["rs"]]
#
#[n for n in routenames["nv"] if not (n in routenames["rs"])]
#
#
#
## Regional- und Fernverkehr haben große Ueberlappung, natuerlich fuehrt das zu Problemen. Was ist hier bitte los?
#
## function to load/count stops
#def loadGtfsStationCounts(scope, path = rawdatadir):
##    scope = "fv"
#    stops = pd.read_csv(path + scope + "/stops.txt")
#    stop_counts = pd.read_csv(
#            # read trip_id and stop_id from stop_times.txt
#            path + scope + "/stop_times.txt", usecols = ["stop_id","trip_id"]
#        # group, and count stops with same stop_id
#        ).groupby('stop_id').count().rename({"trip_id":"n"},axis=1).reset_index()
#    
#    print("STOPS")
#    print(len(stops))
#    print("STOP.COUNTS")
#    print(len(stop_counts))
#    
#    stop_spacecounts = stops.merge(stop_counts, how="right", on = "stop_id")
#    return(stop_spacecounts)

    
##loadGtfsStationCounts("fv")
#
## loop over files and write out
#for scope in outfiles.keys(): # for each level we have data for
#    print(scope) # where are we at?
#    loadGtfsStationCounts(scope).to_csv(outfiles[scope]) 
#    # on the fly (so as not to keep it in memory), load, join, and write out


# for FV and for all:
    # load stop_times
    # if subgroup:
        # filter by route_short_name

    # count per stop_id
    # merge onto stops
