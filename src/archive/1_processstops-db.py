#!/usr/bin/env python
# coding: utf-8

# In[1]:


# load libraries
import pandas as pd
import os, re
import datetime as dt
from sqlalchemy import create_engine, text
import zipfile


# In[2]:


# Welches Jahr?
jahr = "2021"
# Welcher Zip?
zipname = "20211015_fahrplaene_gesamtdeutschland_gtfs"
# # Welche Routenreferenz? (im raw-directory)
routescope = ""


# In[3]:


# define paths
workingdir = "/home/jupyter-maita.schade/VW_Data_Hub/Gap_Map/"
#storagedir = "smb://192.168.90.30/allmende%20verkehr/4%20Projekte/2%20Projekte%20Mobilitätswende/ÖV-Deutschlandkarte%20(Gap-Map)/Berechnungen/raw/gtfs/"


# In[ ]:


# constructed paths
# rawdir = workingdir + "raw/"
rawdir = workingdir + "raw/"
rawdatadir = rawdir + "gtfs/" + 'delfi/'# + jahr + "/"
outdir = workingdir + "out/" + 'delfi/'# + jahr + "/"
#inpath = "{0}{1}_{2}.db".format(rawdatadir,jahr,datum)
zippath = rawdatadir + zipname + ".zip"


# In[7]:


# set up zip file as default for functions
zf = zipfile.ZipFile(zippath) # this is the raw stuff


# ### Filter for routes

# In[8]:


# def getRouteShortNames(scope):
#     # Relying on pre-separated routes file in raw directory
#     # takes scope prefix and gets short_names to filter for
#     # !!! This only works for Fernverkehr!!!
#     print("Scope for routes: "+ scope)
#     routespath = [s for s in os.listdir(rawdatadir) if re.search("routes_"+scope, s) ][0]
#     print("Reading good routes from " + routespath)
#     scope_routes_df = pd.read_csv(rawdatadir + routespath)
#     routenames = scope_routes_df.route_short_name.unique()
# #    routeids = routes_df.route_id.unique()
#     return(routenames)


# In[9]:


# def filterByRoute(trips_df, scope = routescope, zf = zf):
#     # Given a list of route_short_names included in scope
#     # relying on or taking routes and trips in rawdatadir
#     # takes stop_times and filters them to include only stops made on routes included in scope
#     if scope != "":
#         routenames = getRouteShortNames(scope)
#         print("Filtering routes...")
#         routes_df = pd.read_csv(zf.open("routes.txt"), usecols = ["route_short_name", "route_id"])

#         trips_df_filtered = trips_df.merge(
#             routes_df[routes_df["route_short_name"].isin(routenames)], # which routes are ok?
#             how="right",
#             on ="route_id"
#         ) # which trips are on those routes?
# #         print("length now: ", len(trips_df_filtered))
#     else:
#         print("Not filtering routes...")
#         trips_df_filtered = trips_df
#     print("Total trips: ", len(trips_df_filtered))
        
#     return(trips_df_filtered[["trip_id","service_id"]])


# ### Generate counts for `service_id`s

# In[10]:


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
        
#     print(weekdays)

    ref = dt.date.today()                    # choose a reference date
#     print(ref)
    ref -= dt.timedelta(days=ref.weekday())  # and normalize its weekday
#     print(ref)

    # sum up all selected weekdays (max 7 iterations)
#     ct = 0
#     for weekday in weekdays:
#         ref_plus = ref + dt.timedelta(days=weekday)
#         inc = (ref_plus - start).days // 7 - (ref_plus - end).days // 7
#         ct += inc
        
#     print(ct)

    return sum((ref_plus - start).days // 7 - (ref_plus - end).days // 7
               for ref_plus in
               (ref + dt.timedelta(days=weekday) for weekday in weekdays))

def countDaysInIntervalHelper(calendarrow):
    # function to find number of days of service operation based on calendars.txt-entry
    servicepattern = calendarrow.loc["monday":"sunday"].to_numpy()
#     print(servicepattern)
    servicedays = servicepattern.nonzero()[0].tolist()
#     print(servicedays)
    startdate = dt.datetime.strptime(str(int(calendarrow.get("start_date"))),"%Y%m%d")
    enddate = dt.datetime.strptime(str(int(calendarrow.get("end_date"))),"%Y%m%d")
#    if enddate < startdate:
#        print("switched start and end at ", calendarrow.get("service_id"))
    return(interveningWeekdays(startdate, enddate, weekdays = servicedays))

### Helper function to compare dates
def isInIntervalHelper(n, interval):
    '''works only on ARRAY-like n'''
    return(np.where((n <= max(interval)) & (n >= min(interval)), True, False))

# In[11]:


# function to add frequencies... let's hope this is right
def getServiceCount(zf = zf):
    # enriches stop_times DataFrame with information about how often in the feed
    # period each stop is made
    

    print("Getting number of service days for each service")
    # use service_id to find service...
    

    # get regular service from calendar.txt
    print("\t...reading regular service calendars")
    calendar_df = pd.read_csv(zf.open("calendar.txt"))
    calendar_df["days_count"] = calendar_df.apply(countDaysInIntervalHelper, axis=1)

#     calendar_df.to_sql("calendar",db, if_exists = "replace")
    # and get exceptions from calendar_dates.txt

    print("\t...reading calendar exceptions")
    calendar_dates_df = pd.read_csv(zf.open("calendar_dates.txt"))
    

    print("\t...aggregating calendar")
    calendar_df = calendar_dates_df.groupby(["service_id", "exception_type"], as_index=False
                              ).count(
                            ).pivot(index = "service_id", columns = "exception_type", values = "date"
                            ).reset_index(
                            ).merge(calendar_df, on="service_id", how="right"
                            )[['service_id', 1, 2, 'monday',
                                  'tuesday',  'wednesday',   'thursday',     'friday',   'saturday',
                                  'sunday', 'start_date',   'end_date', 'days_count']]
    
    print("\t...calculating total in calendar")
    calendar_df.days_count= (calendar_df.days_count + calendar_df[1].fillna(0) - calendar_df[2].fillna(0)
                            )
    
    return(calendar_df[["service_id","days_count"]])


# ### add counts to trips

# In[12]:


def addCountsToTrips(trips_df, service_count_df):
    trip_counts_df = trips_df.merge(service_count_df[["days_count","service_id"]], how="left", on="service_id")
    return(trip_counts_df[["trip_id","days_count"]])


# In[13]:


def readTrips(zf = zf):
    print("\t...reading trips")
    trips_df = pd.read_csv(zf.open("trips.txt"), usecols = ["route_id","trip_id","service_id"])
    print("\t...", len(trips_df))
    return(trips_df)


# ### add counts to stop_times--chunked

# In[14]:


def countStopTimes(trip_counts_df, dbout, zf = zf):
    start = dt.datetime.now()
    chunksize = 200000
    j = 0
    #     index_start = 1

    print("\t...merging stop_times with trips")
    for df in pd.read_csv(zf.open("stop_times.txt"), chunksize=chunksize, iterator=True, encoding='utf-8'):

        j+=1
    #     print(j)
        if j%10==0:
            print('\t{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize))

        result_df = df.merge(trip_counts_df, on = "trip_id", how="left"
                        )[["stop_id", "days_count"]]
    #     print("\t...", len(result_df))
        if j==1:
            result_df.to_sql("stop_counts", dbout, if_exists='replace')
        else:
            result_df.to_sql("stop_counts", dbout, if_exists='append')
    return(dbout)


# ### querying grouped counts 

# In[15]:


def queryGroupCounts(dbout):
    print("Getting counts grouped by stop...")
    count_df = pd.read_sql_query('SELECT stop_id, SUM(days_count) AS n '
                      'FROM stop_counts '
                      'GROUP BY stop_id',
                      dbout
                     )
    return(count_df)


# ### get and add days in feed

# In[16]:


def getFeedDays(zf):
    ''' Enriches counted dataframe with average daily count for each stop,
    using the feed's calendar information to infer the number of days
    '''

    
    print("getting n of days in feed")
    # read necessary aux files
    calendar_df = pd.read_csv(zf.open("calendar.txt"))
    calendar_dates_df = pd.read_csv(zf.open("calendar_dates.txt"))
    
    # calculate
    startdate =  min(pd.to_datetime(calendar_df.start_date,format="%Y%m%d"))
    enddate = max(pd.to_datetime(calendar_df.end_date,format="%Y%m%d"))
    excdates = pd.to_datetime(calendar_dates_df.date,format="%Y%m%d")

    firstdate = min(startdate, min(excdates))
    lastdate = max(enddate, max(excdates))

    ndays = (lastdate - firstdate).days
    
    return(ndays)


# In[18]:


def addFeedDays(count_df, total_days):
    print("Adding average daily count...")
    count_df["n_day"] = count_df.n/total_days
    return(count_df)


# ### Add stop locations

# In[19]:


def addStopLocations(count_df, zf = zf):
    print("Adding stop locations...")
    stops_df = pd.read_csv(zf.open("stops.txt"))
    located_df = stops_df.merge(count_df, how="right", on="stop_id")
    return located_df


# ### String it together

# In[31]:


def process(routescope, total_days):
    # choose scope-based output connection
    outpath = "{0}{1}{2}.db".format(outdir,zipname,routescope)
    # set up DB connection
    dbout = create_engine('sqlite:///' + outpath)
    csvout = "{}{}{}.nstops.csv".format(outdir,zipname,routescope)
    
    addStopLocations(
        addFeedDays(
            queryGroupCounts(
                countStopTimes(
                    addCountsToTrips(
                        filterByRoute(
                            readTrips(), 
                            scope = routescope
                        ),
                        getServiceCount()
                    ),
                    dbout
                )
            ),
            total_days
        )
    ).to_csv(csvout)
    
    print("Wrote result to " + csvout)


# ## Do it

# In[32]:


ndays = getFeedDays(zf)


# In[33]:



# process("fv", ndays)
process("", ndays)


# In[21]:


# # choose scope-based output connection
# outpath = "{0}{1}{2}.db".format(outdir,zipname,routescope)
# # set up DB connection
# dbout = create_engine('sqlite:///' + outpath)


# In[29]:


# addStopLocations(
#     addFeedDays(
#         queryGroupCounts(dbout),
#         total_days
#     )
# ).to_csv("{}{}{}.nstops.csv".format(outdir,zipname,routescope))

