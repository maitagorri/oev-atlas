#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gtfs_kit as gk
import pandas as pd
import geopandas as gpd
from datetime import datetime


# In[9]:


def cut_dates(gtf, window, validate=True):
    """
    Function to cut a gtfs_kit feed by dates; returns a gtfs_kit feed.

    gtf is a valid gtfs_kit feed
    window is a list or tuple of date strings in format "yyyymmdd"
    validate is a boolean indicating whether the resulting feed should be validated (default True)
    
    Notes: likely loses non-platform/non-stop-time transfers
    """

    # I think we need to...
    # - filter calendar services by window
    # - filter calendar_dates exceptions by window
    # - for each orphaned exception of type 1
    #     - create new calendar service of same ID and drop first exception
    #     - if there is more than one, leave it as exception

    # filter calendar services by window

    services = gtf.calendar[(gtf.calendar.start_date < window[1]) & (gtf.calendar.end_date > window[0])]

    # - filter calendar_dates exceptions by window

    exceptions = gtf.calendar_dates[(gtf.calendar_dates.date < window[1]) & (gtf.calendar_dates.date > window[0])]

    # - for each orphaned exception of type 1

    exceptions = exceptions.sort_values(by=["service_id", "date"])
    orphans = exceptions[(~exceptions.service_id.isin(services.service_id)) & # is not in new service calendar
                         (exceptions.exception_type == 1)                     # and is ON
                        ]

    first_orphans = orphans.groupby("service_id").first().reset_index()

    #     - create new calendar service of same ID

    def serviceFromException(r):
        s = pd.Series( # create a service that has just one date, and is generally off
            {"monday":0,
             "tuesday":0,
             "wednesday":0,
             "thursday":0,
             "friday":0,
             "saturday":0,
             "sunday":0,
             "start_date": r.date,
             "end_date": r.date,
             "service_id": r.service_id
            }) 
        d = datetime.strptime(r.date, "%Y%m%d").weekday()
        s[d] = r.exception_type # set the weekday of this exception's date to its type
        return(s)

    services_expanded = services.append(
        first_orphans.apply(serviceFromException, axis=1)
    )
    
    print("Calendar reduction: {0:0.1f}%".format(100*(len(gtf.calendar)-len(services_expanded))/len(gtf.calendar)))

    #     - drop first exception from exceptions
    #     - if there is more than one, leave additional ones as exception

    # find which ones are in both, and keep only the rest
    exception_merge = exceptions.merge(first_orphans[['service_id', 'date']], how='outer', indicator='source') 
    exceptions_reduced = exception_merge[exception_merge.source.eq('left_only')].drop('source', axis=1)

    print("Calendar_dates reduction: {0:0.1f}%".format(100*(len(gtf.calendar_dates)-len(exceptions_reduced))/len(gtf.calendar_dates)))

    
    # Now, we ought to filter the rest of the feed.
    # - `trips` by `service_id`
    # - `routes` by `route_id`
    # - `stop_times` by `trip_id`
    # - `stops` by `stop_id`
    # - `agency` by `agency_id`
    # - `transfers` by `stop_id`

    # - `trips` by `service_id`

    trips = gtf.trips[gtf.trips.service_id.isin(services_expanded.service_id)]

    print("Trips reduction: {0:0.1f}%".format(100*(len(gtf.trips)-len(trips))/len(gtf.trips)))

    # - `routes` by `route_id`

    routes = gtf.routes[gtf.routes.route_id.isin(trips.route_id)]

    print("Routes reduction: {0:0.1f}%".format(100*(len(gtf.routes)-len(routes))/len(gtf.routes)))

    # - `stop_times` by `trip_id`

    stop_times = gtf.stop_times[gtf.stop_times.trip_id.isin(trips.trip_id)]

    print("Stop_times reduction: {0:0.1f}%".format(100*(len(gtf.stop_times)-len(stop_times))/len(gtf.stop_times)))

    # - `stops` by `stop_id`
    #     - here it's important to keep their parent stations, also!

    stops = gtf.stops[gtf.stops.stop_id.isin(stop_times.stop_id)]
    # add lost parent stations back in
    stops = stops.append(gtf.stops[gtf.stops.stop_id.isin(stops.parent_station)])

    print("Stops reduction: {0:0.1f}%".format(100*(len(gtf.stops)-len(stops))/len(gtf.stops)))

    # - `agency` by `agency_id` 

    agency = gtf.agency[gtf.agency.agency_id.isin(routes.agency_id)]

    print("Agencies reduction: {0:0.1f}%".format(100*(len(gtf.agency)-len(agency))/len(gtf.agency)))

    # - `transfers` by `stop_id`
    if not (gtf.transfers is None):
        transfers = gtf.transfers[(gtf.transfers.from_stop_id.isin(stops.stop_id)) & (gtf.transfers.to_stop_id.isin(stops.stop_id))]
        print("Transfers reduction: {0:0.1f}%".format(100*(len(gtf.transfers)-len(transfers))/len(gtf.transfers)))
    else:
        transfers = None
    # And new feed_info

    feed_info = pd.DataFrame({
        'feed_publisher_name' : 'Agora Verkehrswende', 
        'feed_publisher_url' : 'https://www.agora-verkehrswende.de/',
        'feed_lang' : 'de',
        'feed_start_date' : window[0],
        'feed_end_date' : window[1], 
        'feed_version' : gtf.feed_info.feed_version + "_cut-"+window[0]+"--"+window[1],
        'feed_contact_email' : "maita.schade@agora-verkehrswende.de", 
        'feed_contact_url' : 'https://www.agora-verkehrswende.de/ueber-uns/team/maita/schade/'
        }, 
        index=[0])

    # Create new feed
    gtf_cut = gk.feed.Feed(dist_units=gtf.dist_units,
                           agency=agency,
                           stops=stops,
                           routes=routes,
                           trips=trips,
                           stop_times=stop_times,
                           calendar=services_expanded,
                           calendar_dates=exceptions_reduced,
                           transfers=transfers,
                           feed_info=feed_info
                          )

    # validate new feed, if not turned off in function call
    if validate:
        print("Validating feed...")
        print(gtf_cut.validate())
    
    # return the cut feed
    return(gtf_cut)


# In[17]:


def cut_geom(gtf, shape, cut_name="geom", validate=True):
    """
    Function to cut a gtfs_kit feed by geometry; returns a gtfs_kit feed.

    gtf is a valid gtfs_kit feed
    shape is a geodataframe of the area(s) we want to restrict the feed to
    cut_name is a string for modifying the version name of the resulting feed (default "geom")
    validate is a boolean indicating whether the resulting feed should be validated (default True)
    """

    # Cut the stops first
    stops_gdf = gpd.GeoDataFrame(gtf.stops.copy(), 
                # copy a dataframe before making a gdf from it, to avoid geometry column in original!
                                 geometry=gpd.points_from_xy(gtf.stops.stop_lon, gtf.stops.stop_lat),
                                 crs="epsg:4326"
                                )

    stops = pd.DataFrame(gpd.sjoin(stops_gdf, shape[["geometry"]], op="within"
                                  )).drop(columns=['geometry', 'index_right'])

    # Make sure all parent stations are there... 
    # There are edge cases that get lost otherwise

    lost_parent_stations = stops[~stops.parent_station.isna() & ~stops.parent_station.isin(stops.stop_id)].parent_station.unique()

    stops = stops.append(gtf.stops[gtf.stops.stop_id.isin(lost_parent_stations)])

    print("Stops reduction: {0:0.1f}%".format(100*(len(gtf.stops)-len(stops))/len(gtf.stops)))

    # Now we have a reduced set of stops and should just be able to trickle our way through the other tables:
    # - `stop_times` by `stop_id`
    # - `trips` by `trip_id`
    # - `calendar` and `calendar_dates` by `service_id`
    # - `routes` by `route_id` 
    # - `agency` by `agency_id`
    # - `transfers` by `stop_id`

    # - `stop_times` by `stop_id`

    stop_times = gtf.stop_times[gtf.stop_times.stop_id.isin(stops.stop_id)]

    print("Stop_times reduction: {0:0.1f}%".format(100*(len(gtf.stop_times)-len(stop_times))/len(gtf.stop_times)))

    # - `trips` by `trip_id`

    trips = gtf.trips[gtf.trips.trip_id.isin(stop_times.trip_id)]

    print("Trips reduction: {0:0.1f}%".format(100*(len(gtf.trips)-len(trips))/len(gtf.trips)))

    # - `calendar` and `calendar_dates` by `service_id`


    calendar = gtf.calendar[gtf.calendar.service_id.isin(trips.service_id)]
    print("Calendar reduction: {0:0.1f}%".format(100*(len(gtf.calendar)-len(calendar))/len(gtf.calendar)))


    calendar_dates = gtf.calendar_dates[gtf.calendar_dates.service_id.isin(trips.service_id)]
    print("Calendar_dates reduction: {0:0.1f}%".format(100*(len(gtf.calendar_dates)-len(calendar_dates))/len(gtf.calendar_dates)))

    # - `routes` by `route_id` 


    routes = gtf.routes[gtf.routes.route_id.isin(trips.route_id)]
    print("Routes reduction: {0:0.1f}%".format(100*(len(gtf.routes)-len(routes))/len(gtf.routes)))

    # - `agency` by `agency_id`

    agency = gtf.agency[gtf.agency.agency_id.isin(routes.agency_id)]
    print("Agencies reduction: {0:0.1f}%".format(100*(len(gtf.agency)-len(agency))/len(gtf.agency)))

    # - `transfers` by `stop_id`
    
    if not (gtf.transfers is None):
        transfers = gtf.transfers[(gtf.transfers.from_stop_id.isin(stops.stop_id)) & (gtf.transfers.to_stop_id.isin(stops.stop_id))]
        print("Transfers reduction: {0:0.1f}%".format(100*(len(gtf.transfers)-len(transfers))/len(gtf.transfers)))
    else:
        transfers = None

    # And new feed_info

    feed_info = pd.DataFrame({
        'feed_publisher_name' : 'Agora Verkehrswende', 
        'feed_publisher_url' : 'https://www.agora-verkehrswende.de/',
        'feed_lang' : 'de',
        'feed_start_date' : gtf.feed_info.feed_start_date,
        'feed_end_date' : gtf.feed_info.feed_end_date, 
        'feed_version' : gtf.feed_info.feed_version + "_cut-"+cut_name,
        'feed_contact_email' : "maita.schade@agora-verkehrswende.de", 
        'feed_contact_url' : 'https://www.agora-verkehrswende.de/ueber-uns/team/maita/schade/'
        }, 
        index=[0])

    # Create new feed
    gtf_cut = gk.feed.Feed(dist_units=gtf.dist_units,
                           agency=agency,
                           stops=stops,
                           routes=routes,
                           trips=trips,
                           stop_times=stop_times,
                           calendar=calendar,
                           calendar_dates=calendar_dates,
                           transfers=transfers,
                           feed_info=feed_info
                          )

    # validate new feed, if not turned off in function call
    if validate:
        print("Validating feed...")
        print(gtf_cut.validate())

    # return the cut feed
    return(gtf_cut)

