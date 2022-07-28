#!/usr/bin/env python
# coding: utf-8

# In[1]:


# load libraries
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text
import zipfile
import re
import sys

# sys-input
filename = sys.argv[1]
zipname = re.search('([^/]+).zip', filename)[1]
print(zipname)

# ## Setup

# In[2]:


# Welcher Zip?
source = "delfi" # where is the feed file? delfi or gtfs.de?
# zipname = '20220214_fahrplaene_gesamtdeutschland_gtfs' # name of GTFS zipfile

# define paths
workingdir = "../../data/" 
#storagedir = "smb://192.168.90.30/allmende%20verkehr/4%20Projekte/2%20Projekte%20Mobilitätswende/ÖV-Deutschlandkarte%20(Gap-Map)/Berechnungen/raw/gtfs/"

# constructed paths
rawdir = workingdir + "raw/" # where is all the data?
gtfsdir = rawdir + source + "/" # where zip-file is located
outdir = workingdir + "interim/" # where do outputfiles go?
zippath = gtfsdir + zipname + ".zip"

# set up zip file as default for functions
zf = zipfile.ZipFile(zippath) # this is the raw stuff


# In[3]:


# choose file-based output connection
outpath = '{0}{1}.db'.format(outdir,zipname)
# set up DB connection
dbout = create_engine('sqlite:///' + outpath)


# In[4]:


# file for logging
logfile = outdir + zipname + ".log"
with open(logfile, 'a') as f:
    f.write('## Eckdaten\n')


# # Count service_ids

# In[5]:


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

    return sum((ref_plus - start).days // 7 - (ref_plus - end).days // 7
               for ref_plus in
               (ref + dt.timedelta(days=weekday) for weekday in weekdays))

def countDaysInIntervalHelper(calendarrow):
    # function to find number of days of service operation based on calendars.txt-entry
    servicepattern = calendarrow.loc["monday":"sunday"].to_numpy()
    servicedays = servicepattern.nonzero()[0].tolist()

    startdate = dt.datetime.strptime(str(int(calendarrow.get("start_date"))),"%Y%m%d")
    enddate = dt.datetime.strptime(str(int(calendarrow.get("end_date"))),"%Y%m%d")
    return(interveningWeekdays(startdate, enddate, weekdays = servicedays))

### Helper function to compare dates
def isInIntervalHelper(n, interval):
    '''works only on ARRAY-like n'''
    return(np.where((n <= max(interval)) & (n >= min(interval)), True, False))


# In[6]:


# function to add frequencies...
def addCountToCalendar(calendar_df, calendar_dates_df):
    # enriches stop_times DataFrame with information about how often in the feed
    # period each stop is made
    

    print("Getting number of service days for each service")
    # use service_id to find service...
    calendar_df["days_count"] = calendar_df.apply(countDaysInIntervalHelper, axis=1)    

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
    
    return(calendar_df)


# In[7]:


def feedDays(calendar_df, calendar_dates_df):
    ''' Enriches counted dataframe with average daily count for each stop,
    using the feed's calendar information to infer the number of days
    '''
    # calculate
    startdate =  min(pd.to_datetime(calendar_df.start_date,format="%Y%m%d"))
    enddate = max(pd.to_datetime(calendar_df.end_date,format="%Y%m%d"))
    excdates = pd.to_datetime(calendar_dates_df.date,format="%Y%m%d")

    firstdate = min(startdate, min(excdates))
    lastdate = max(enddate, max(excdates))

    ndays = (lastdate - firstdate).days
    with open(logfile, 'a') as f:
        f.write('First date:\t{}\n'.format(firstdate))
        f.write('Last date:\t{}\n'.format(lastdate))
        f.write('Total days:\t{}\n\n'.format(ndays))
    print('First date:\t',firstdate)
    print('Last date:\t',lastdate)
    print('Total days:\t',ndays)
    return(ndays)


# In[8]:


calendar_df = pd.read_csv(zf.open("calendar.txt"))
calendar_dates_df = pd.read_csv(zf.open("calendar_dates.txt"))


# In[9]:


calendar_df = addCountToCalendar(calendar_df, calendar_dates_df)


# In[10]:


ndays = feedDays(calendar_df, calendar_dates_df) # total number of days in feed period


# # Pick out routes

# This adds a FZ and FB-flag to the routes file, based on previous filtering of FV/NV-routes

# In[11]:


fz_routes = pd.read_csv(outdir + zipname + '_fz-routes.csv').route_id
fb_routes = pd.read_csv(outdir + zipname + '_fb-routes.csv').route_id

routes_df = pd.read_csv(zf.open("routes.txt"))

routes_df['fz'] = routes_df.route_id.isin(fz_routes)
routes_df['fb'] = routes_df.route_id.isin(fb_routes)


# # Get things into database

# ## calendar

# In[12]:


# put enriched calendar into database
calendar_df.to_sql("calendar", 'sqlite:///' + outpath,
          if_exists = 'replace')


# ## routes

# In[13]:


routes_df.to_sql("routes", 'sqlite:///' + outpath,
          if_exists = 'replace')


# ## trips, stops, et al.--everything that goes straight into DB

# Transfer gtfs-files into database in chunks

# In[14]:
start = dt.datetime.now()
chunksize = 200000

ziptables = ['stops','trips', 'stop_times']
    
    
for table_name in ziptables:
    print(table_name)

    j=0
    for df in pd.read_csv(zf.open(table_name + ".txt"),
                          chunksize=chunksize, iterator=True, encoding='utf-8',
                           dtype={'Unnamed: 0': 'float64',
                           'drop_off_type': 'object',
                           'pickup_type': 'object',
                           'stop_sequence': 'object',
                           'trip_id': 'object',
                           'stop_headsign': 'object'}
                         ):
        j+=1
        if j%10==0: # track progress visibly
            print('\t{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize))

        if j==1:
            df.to_sql(table_name, dbout, if_exists='replace')
        else:
            df.to_sql(table_name, dbout, if_exists='append')

# # Database Merging

# ### recovery if crashed on db create

# * run setup (without logging file)
# * get feed days

# Count stop_times per stop using SQL (to keep large files out of working memory)

# This is where the filtering by route happens--therefore two different queries, one for each route subset

# Zug:

# In[15]:


print(dt.datetime.now())

fz_out_df = pd.read_sql_query(
    'SELECT n_stops.stop_id, n, stop_name, parent_station, stop_lat, stop_lon, location_type '
    'FROM ('
        'SELECT stop_id, SUM(days_count) AS n '
        'FROM ('
            'SELECT routes.route_short_name, routes.route_id, trips.service_id, trips.trip_headsign, trips.direction_id, trips.trip_id '
            'FROM routes '
            'LEFT JOIN trips ON routes.route_id = trips.route_id '
            'WHERE routes.fz IS 1 '
        ') AS trips_fz '
        'LEFT JOIN stop_times ON trips_fz.trip_id = stop_times.trip_id '
        'LEFT JOIN calendar ON trips_fz.service_id = calendar.service_id '
        'GROUP BY stop_id '
    ') AS n_stops '
    'JOIN stops ON n_stops.stop_id = stops.stop_id',
    dbout
)

# Bus:

# In[16]:


fz_out_df["n_day"] = fz_out_df.n/ndays # the count per day, for comparing different length feeds
fz_out_df.to_csv(outdir + zipname + ".fz.nstops.csv")


# In[17]:

print(dt.datetime.now())

fb_out_df = pd.read_sql_query(
    'SELECT n_stops.stop_id, n, stop_name, parent_station, stop_lat, stop_lon, location_type '
    'FROM ('
        'SELECT stop_id, SUM(days_count) AS n '
        'FROM ('
            'SELECT routes.route_short_name, routes.route_id, trips.service_id, trips.trip_headsign, trips.direction_id, trips.trip_id '
            'FROM routes '
            'LEFT JOIN trips ON routes.route_id = trips.route_id '
            'WHERE routes.fb IS 1 '
        ') AS trips_fb '
        'LEFT JOIN stop_times ON trips_fb.trip_id = stop_times.trip_id '
        'LEFT JOIN calendar ON trips_fb.service_id = calendar.service_id '
        'GROUP BY stop_id '
    ') AS n_stops '
    'JOIN stops ON n_stops.stop_id = stops.stop_id',
    dbout
)

# In[18]:


fb_out_df["n_day"] = fb_out_df.n/ndays # the count per day, for comparing different length feeds
fb_out_df.to_csv(outdir + zipname + ".fb.nstops.csv")


# Nahverkehr:

# In[19]:

print(dt.datetime.now())

nv_out_df = pd.read_sql_query(
    'SELECT n_stops.stop_id, n, stop_name, parent_station, stop_lat, stop_lon, location_type '
    'FROM ('
        'SELECT stop_id, SUM(days_count) AS n '
        'FROM ('
            'SELECT routes.route_short_name, routes.route_id, trips.service_id, trips.trip_headsign, trips.direction_id, trips.trip_id '
            'FROM routes '
            'LEFT JOIN trips ON routes.route_id = trips.route_id '
            'WHERE routes.fz IS 0 AND routes.fb IS 0'
        ') AS trips_nv '
        'LEFT JOIN stop_times ON trips_nv.trip_id = stop_times.trip_id '
        'LEFT JOIN calendar ON trips_nv.service_id = calendar.service_id '
        'GROUP BY stop_id '
    ') AS n_stops '
    'JOIN stops ON n_stops.stop_id = stops.stop_id',
    dbout
)

# In[20]:


nv_out_df["n_day"] = nv_out_df.n/ndays # the count per day, for comparing different length feeds
nv_out_df.to_csv(outdir + zipname + ".nv.nstops.csv")


# # Wrap up and write out
