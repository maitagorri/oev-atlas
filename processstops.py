#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 23:22:49 2021

@author: maita
"""
# load libraries
import pandas as pd
import os, re
import datetime as dt

# Welches Jahr?
jahr = "2020"

# define paths
workingdir = "/mnt/c/Users/maita.schade/Nextcloud/Documents/Work/Gap_Map/"
# workingdir = "/home/maita/Nextcloud/Documents/Work/Gap_Map/"
rawdir = workingdir + "raw/"
rawdatadir = rawdir + "gtfs/" + jahr + "/"
outdir = workingdir + "out/"+jahr+"/"


def getRouteShortNames(scope):
    # Relying on pre-separated routes file in raw directory
    # takes scope prefix and gets short_names to filter for
    # !!! This only works for Fernverkehr!!!
    print("Scope for routes: "+ scope)
    routespath = [s for s in os.listdir(rawdatadir) if re.search("routes_"+scope, s) ][0]
    routes_df = pd.read_csv(rawdatadir + routespath)
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
    if "days_count" in stop_times_df.columns:
        return(stop_times_df.groupby('stop_id').sum().rename({"days_count":"n"},axis=1).reset_index()[['stop_id','n']])
    else:
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
    
def interveningWeekdays(start, end, inclusive=True, weekdays=[0, 1, 2, 3, 4]):
    # a useful function from Stackoverflow, to count particular weekdays in date range
    if isinstance(start, dt.datetime):
        start = start.date()               # make a date from a datetime

    if isinstance(end, dt.datetime):
        end = end.date()                   # make a date from a datetime

    if end < start:
        # you can opt to return 0 or swap the dates around instead
        # raise ValueError("start date must be before end date")
        end, start = start, end

    if inclusive:
        end += dt.timedelta(days=1)  # correct for inclusivity

    try:
        # collapse duplicate weekdays
        weekdays = {weekday % 7 for weekday in weekdays}
    except TypeError:
        weekdays = [weekdays % 7]

    ref = dt.date.today()                    # choose a reference date
    ref -= dt.timedelta(days=ref.weekday())  # and normalize its weekday

    # sum up all selected weekdays (max 7 iterations)
    return sum((ref_plus - start).days // 7 - (ref_plus - end).days // 7
               for ref_plus in
               (ref + dt.timedelta(days=weekday) for weekday in weekdays))

def countDaysInIntervalHelper(calendarrow):
    # function to find number of days of service operation based on calendars.txt-entry
    servicedays = calendarrow[0:7].to_numpy().nonzero()[0].tolist()
    startdate = dt.datetime.strptime(str(calendarrow.get("start_date")),"%Y%m%d")
    enddate = dt.datetime.strptime(str(calendarrow.get("end_date")),"%Y%m%d")
#    if enddate < startdate:
#        print("switched start and end at ", calendarrow.get("service_id"))
    return(interveningWeekdays(startdate, enddate, weekdays = servicedays))
    
def addFrequency(stop_times_df, 
                 trips_path = rawdatadir + "trips.txt", 
                 calendar_path = rawdatadir + "calendar.txt",
                 calendar_dates_path = rawdatadir + "calendar_dates.txt"):
    # enriches stop_times DataFrame with information about how often in the feed
    # period each stop is made
    
    print("Getting number of service days for each stop_time")
    # use service_id to find service...
    # get regular service from calendar.txt
    print("\t...reading regular service calendars")
    calendar_df = pd.read_csv(calendar_path)
    calendar_df["days_count"] = calendar_df.apply(countDaysInIntervalHelper, axis=1)
    # and get exceptions from calendar_dates.txt
    
    print("\t...reading calendar exceptions")
    calendar_dates_df = pd.read_csv(calendar_dates_path)
    print("\t...aggregating calendar")
    calendar_df = calendar_dates_df.groupby(["service_id", "exception_type"], as_index=False
                              ).count(
                            ).pivot(index = "service_id", columns = "exception_type", values = "date"
                            ).reset_index(
                            ).merge(calendar_df, on="service_id", how="right")
    
    print("\t...calculating total in calendar")
    calendar_df.days_count= calendar_df.days_count + calendar_df[1].fillna(0) - calendar_df[2].fillna(0)

    # use trip_id to look up associated trip
    # from trip, look up service_id
    print("\t...reading trips")
    trips_df = pd.read_csv(trips_path)
    print("\t...", len(trips_df))
    print("\t...merging stop_times with trips")
    result_df = stop_times_df.merge(trips_df[["trip_id","service_id"]], on = "trip_id", how="left"
                    )[["trip_id","stop_id", "service_id"]]
    print("\t...", len(result_df))
    
    print("\t...merging id'ed stop_times with calendar")
    result_df = result_df.merge(calendar_df[["service_id", "days_count"]], on = "service_id", how = "left")[["trip_id","stop_id", "days_count"]]
    print("\t...", len(result_df))
    print("\t...returning the result")
    
    return(result_df)

def addFrequency2(stop_times_df, 
                 trips_path = rawdatadir + "trips.txt", 
                 calendar_path = rawdatadir + "calendar.txt",
                 calendar_dates_path = rawdatadir + "calendar_dates.txt"):
    # enriches stop_times DataFrame with information about how often in the feed
    # period each stop is made
    
    # some helper functions
    def serviceToDatesHelper(r): 
        '''Helper function that returns a list of date strings, given a calendar row
            with weekdays, start and end dates
        '''
        # day suffixes to be able to use pd.date_range to get a list of dates
        day_suff = ["MON","TUE","WED","THU","FRI","SAT","SUN"]
        # get relevant information from the row
        weekdays = [i for i,v in enumerate(r[0:7]) if v!=0]
        start = dt.datetime.strptime(str(r.start_date),"%Y%m%d").date()
        end = dt.datetime.strptime(str(r.end_date),"%Y%m%d").date()

        # generate the date strings
        active_dates = [
            i for l in [
                # get a date range of all dates...
                pd.date_range(start=start,end=end,freq="W-"+day_suff[d]
                              # ...for each weekday contained in service
                          ).strftime("%Y%m%d") for d in weekdays
                ] for i in l] #flatten this list situation

        return(active_dates)

    def serviceToExcHelperLoopy(r):
        '''Same as above, but not returning a dataframe, rather two same-length lists
            (exc-type can be added later, since it is always 1)
        '''
        active_dates = serviceToDatesHelper(r)
        service_id = [r["service_id"]]*len(active_dates)

        return(service_id, active_dates)
    
    def calendarToDates(df):
        '''Takes a calendar.txt-df and returns a calendar_dates.txt-style df'''
        ids, dates = [], []
        for _, row in df.iterrows():
            i, d = serviceToExcHelperLoopy(row)
            ids += i
            dates += d

        df_out = pd.DataFrame({'service_id': ids,
                              'exception_type': [1] * len(ids),
                              'date': dates})
        df_out.date = df_out.date.astype(int)
        return(df_out)
    
    print("Getting number of service days for each stop_time")
    # use service_id to find service...
    # get regular service from calendar.txt
    print("\t...reading regular service calendars")
    calendar_df = pd.read_csv(calendar_path)
    # and get exceptions from calendar_dates.txt
    print("\t...reading calendar exceptions")
    calendar_dates_df = pd.read_csv(calendar_dates_path)
    print("\t...aggregating calendar")
    # convert calendar to dates format
    calendar_as_dates_df = calendarToDates(calendar_df)
    # join exceptions with regular dates
    all_dates_df = calendar_as_dates_df.merge(calendar_dates_df, on=["service_id",'date'], how='outer')
    # pick out dates that have service and were not canceled, or were added
    service_dates_df = all_dates_df[((all_dates_df.exception_type_x==1) & (all_dates_df.exception_type_y!=2)) | # regular trips
                               ((all_dates_df.exception_type_x!=1) & (all_dates_df.exception_type_y==1))   # exceptional additions
                   ]    
    print("\t...calculating total in calendar")
    daycounts_df = service_dates_df.groupby("service_id",as_index=False
                                           ).sum(
                                           ).rename({"exception_type_x":"days_count"},axis=1
                                           )[["service_id","days_count"]]

    # use trip_id to look up associated trip
    # from trip, look up service_id
    print("\t...reading trips")
    trips_df = pd.read_csv(trips_path)
    print("\t...", len(trips_df))
    print("\t...merging stop_times with trips")
    result_df = stop_times_df.merge(trips_df[["trip_id","service_id"]], on = "trip_id", how="left"
                    )[["trip_id","stop_id", "service_id"]]
    print("\t...", len(result_df))
    
    print("\t...merging id'ed stop_times with calendar")
    result_df = result_df.merge(daycounts_df, on = "service_id", how = "left")[["trip_id","stop_id", "days_count"]]
    print("\t...", len(result_df))
    print("\t...returning the result")
    
    return(result_df)

def addPerDay(counted_df,
              calendar_path = rawdatadir + "calendar.txt",
              calendar_dates_path = rawdatadir + "calendar_dates.txt"
             ):
    ''' Enriches counted dataframe with average daily count for each stop,
    using the feed's calendar information to infer the number of days
    '''
    
    print("Adding average daily count to counted df")
    # read necessary aux files
    calendar_df = pd.read_csv(calendar_path)
    calendar_dates_df = pd.read_csv(calendar_dates_path)
    
    # calculate
    startdate =  min(pd.to_datetime(calendar_df.start_date,format="%Y%m%d"))
    enddate = max(pd.to_datetime(calendar_df.end_date,format="%Y%m%d"))
    excdates = pd.to_datetime(calendar_dates_df.date,format="%Y%m%d")

    firstdate = min(startdate, min(excdates))
    lastdate = max(enddate, max(excdates))

    ndays = (lastdate - firstdate).days
    
    try:
        counted_df["n_day"] = counted_df.n / ndays
    
    except:
        print("No count found in df passed to per-day-function")
        pass
    
    return counted_df

        
# Count and write out all stops per location
addLocationsToStops(
    addPerDay(
            countPerStop(
                    addFrequency(
                            readStopTimes(rawdatadir)
                            )
                    )
            )
        ).to_csv(outdir + "210720_nstops.csv")

# Count and write out only FV-stops per location
addLocationsToStops(
        addPerDay(
            countPerStop(
                addFrequency(
                    filterByRoute(
                            readStopTimes(rawdatadir), 
                            getRouteShortNames("fv"))
                    )
                )
            )
        ).to_csv(outdir + "210720_fv.nstops.csv")

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
#allroutes = pd.read_csv(rawdatadir + "routes", usecols = ["route_short_name", "route_long_name", "route_id"])
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
# screwy services in calendar.txt
#switched start and end at  48690
#switched start and end at  39678
#switched start and end at  70243
#switched start and end at  50649
#switched start and end at  3709
#switched start and end at  41322
#switched start and end at  56831
#switched start and end at  8555
#switched start and end at  67418
#switched start and end at  14299
#switched start and end at  56521
#switched start and end at  24379
#switched start and end at  85349
#switched start and end at  17231
#switched start and end at  62387
#switched start and end at  48438
#switched start and end at  8824
#switched start and end at  11382
#switched start and end at  36763
#switched start and end at  14501
#switched start and end at  13351
#switched start and end at  18270
#switched start and end at  24255
#switched start and end at  88503
#switched start and end at  58819
#switched start and end at  59504
#switched start and end at  66667
#switched start and end at  2783
#switched start and end at  92231
#switched start and end at  33342
#switched start and end at  76859
#switched start and end at  35238
#switched start and end at  45059
#switched start and end at  15998
#switched start and end at  30623
#switched start and end at  10013
#switched start and end at  28065
#switched start and end at  84566
#switched start and end at  52409
#switched start and end at  17057
#switched start and end at  39914
#switched start and end at  57899
#switched start and end at  4954
#switched start and end at  13269
#switched start and end at  1888
#switched start and end at  47868
#switched start and end at  58814
#switched start and end at  80388
#switched start and end at  50269
#switched start and end at  94098
#switched start and end at  64844
#switched start and end at  16803
#switched start and end at  28939
#switched start and end at  81996
#switched start and end at  85045
#switched start and end at  41463
#switched start and end at  78571
#switched start and end at  37424
#switched start and end at  80157
#switched start and end at  2266
#switched start and end at  70815
#switched start and end at  79583
#switched start and end at  52256
#switched start and end at  62241
#switched start and end at  71292
#switched start and end at  62549
#switched start and end at  33296
#switched start and end at  80126
#switched start and end at  67793
#switched start and end at  16960
#switched start and end at  59449
#switched start and end at  59086
#switched start and end at  27519
#switched start and end at  53982
#switched start and end at  6038
#switched start and end at  61519
#switched start and end at  50962
#switched start and end at  35949
#switched start and end at  7163
#switched start and end at  42795
#switched start and end at  82621
#switched start and end at  28830
#switched start and end at  35698
#switched start and end at  78859
#switched start and end at  16626
#switched start and end at  35794
#switched start and end at  91702
#switched start and end at  20713
#switched start and end at  32449
#switched start and end at  32825
#switched start and end at  61694
#switched start and end at  11104
#switched start and end at  50825
#switched start and end at  18433
#switched start and end at  81816
#switched start and end at  47270
#switched start and end at  28853
#switched start and end at  59946
#switched start and end at  53533
#switched start and end at  25322
#switched start and end at  22760
#switched start and end at  24742
#switched start and end at  630
#switched start and end at  81350
#switched start and end at  34354
#switched start and end at  37005
#switched start and end at  81968
#switched start and end at  87624
#switched start and end at  77226
#switched start and end at  90219
#switched start and end at  34206
#switched start and end at  58243
#switched start and end at  78240
#switched start and end at  49692
