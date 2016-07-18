
import os
import json

import fiona
import rasterio
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import datetime
import calendar
from dateutil.relativedelta import relativedelta


# --------------------------------------------------
# spatial functions

def envelope_to_scale(env):
    """Get scale info from envelope
    """
    # check bbox size
    xsize = env[2][0] - env[1][0]
    ysize = env[0][1] - env[1][1]
    tsize = abs(xsize * ysize)

    scale = "regional"
    if tsize >= 32400:
        scale = "global"

    return scale


def envelope_to_geom(env):
    """convert envelope array to geojson
    """
    geom = {
        "type": "Polygon",
        "coordinates": [ [
            env[0],
            env[1],
            env[2],
            env[3],
            env[0]
        ] ]
    }
    return geom


def trim_envelope(env):
    """Trim envelope to global extents
    """
    # clip extents if they are outside global bounding box
    for c in range(len(env)):
        if env[c][0] < -180:
            env[c][0] = -180

        elif env[c][0] > 180:
            env[c][0] = 180

        if env[c][1] < -90:
            env[c][1] = -90

        elif env[c][1] > 90:
            env[c][1] = 90

    return env


def check_envelope(new, old):
    """expand old envelope to max extents of new envelope
    """
    if len(new) == len(old) and len(new) == 4:
        # update envelope if polygon extends beyond bounds

        if new[0] < old[0]:
            old[0] = new[0]
        if new[1] > old[1]:
            old[1] = new[1]
        if new[2] < old[2]:
            old[2] = new[2]
        if new[3] > old[3]:
            old[3] = new[3]

    elif len(old) == 0 and len(new) == 4:
        # initialize envelope
        for x in new:
            old.append(x)


    return old


def raster_envelope(path):
    """Get geojson style envelope of raster file
    """
    raster = rasterio.open(path, 'r')

    # bounds = (xmin, ymin, xmax, ymax)
    b = raster.bounds
    env = [[b[0], b[3]], [b[0], b[1]], [b[2], b[1]], [b[2], b[3]]]

    return env


def vector_envelope(path):
    """Get geojson style envelope of vector file
    """
    vector = fiona.open(path, 'r')

    # bounds = (xmin, ymin, xmax, ymax)
    b = vector.bounds
    env = [[b[0], b[3]], [b[0], b[1]], [b[2], b[1]], [b[2], b[3]]]

    return env


def vector_list(vlist=[]):
    """Get envelope for multiple vector files
    """
    env = []
    for f in vlist:
        f_env = vectory_envelope(f)
        env = check_envelope(f_env, env)

    return env


def point_gen(item):
    """create point from dict with lon, lat

    return a point given a pandas row (or any object) which
       includes longitude and latitude

    return "None" if valid lon,lat not found
    """
    try:
        lon = float(item['longitude'])
        lat = float(item['latitude'])
        return Point(lon, lat)
    except:
        return "None"


def release_envelope(path):
    """create geojson style envelope from csv with lon, lat
    """
    if not os.path.isfile(path):
        quit("Locations table could not be found.")

    try:
        df = pd.read_csv(path, sep=",", quotechar='\"')
    except:
        quit("Error reading locations table.")

    df['geometry'] = df.apply(point_gen, axis=1)
    gdf = gpd.GeoDataFrame(df.loc[df.geometry != "None"])

    # bounds = (xmin, ymin, xmax, ymax)
    b = gdf.total_bounds
    env = [[b[0], b[3]], [b[0], b[1]], [b[2], b[1]], [b[2], b[3]]]

    return env


# -------------------------------------------------------------------------
# resource functions

def gen_nested_release(path=None):
    """Yield nested project dicts for geocoded research releases.

    Convert flat tables from release into nested dicts which can be
    inserted into a mongodb collection.

    Args:
        path (str): path to root directory of Level 1 geocoded
                    research release
    """
    if not os.path.isdir(path):
        quit("Invalid release directory provided.")


    files = ["projects", "locations", 'transactions']

    tables = {}
    for table in files:
        file_path = path+"/data/"+table+".csv"

        if not os.path.isfile(file_path):
            raise Exception("no valid table type found for: " + file_path)

        tables[table] = pd.read_csv(file_path, sep=',', quotechar='\"')
        tables[table]["project_id"] = tables[table]["project_id"].astype(str)


    # add new data for each project
    for project_row in tables['projects'].iterrows():

        project = dict(project_row[1])
        project_id = project["project_id"]

        transaction_match = tables['transactions'].loc[
            tables['transactions']["project_id"] == project_id]

        if len(transaction_match) > 0:
            project["transactions"] = [dict(x[1])
                                       for x in transaction_match.iterrows()]

        else:
            print "No transactions found for project id: " + str(project_id)


        location_match = tables['locations'].loc[
            tables['locations']["project_id"] == project_id]

        if len(location_match) > 0:
            project["locations"] = [dict(x[1])
                                    for x in location_match.iterrows()]

        else:
            print "No locations found for project id: " + str(project_id)


        yield project


def add_asdf_id(path):
    """Adds unique id field (asdf_id) and outputs geojson

    serves as shp to geojson converter as well
    also sets permissions for files
    """
    geo_df = gpd.GeoDataFrame.from_file(path)
    geo_df["asdf_id"] = range(len(geo_df))

    geo_json = geo_df.to_json()
    geo_path = os.path.splitext(path)[0] + ".geojson"
    geo_file = open(geo_path, "w")
    json.dump(json.loads(geo_json), geo_file, indent = 4)
    geo_file.close()
    os.chmod(geo_path, 0664)

    # create simplified geojson for use with leaflet web map
    geo_df['geometry'] = geo_df['geometry'].simplify(0.01)
    simple_geo_path = os.path.dirname(path)+"/simplified.geojson"
    simple_geo_file = open(simple_geo_path, "w")
    json.dump(json.loads(geo_df.to_json()), simple_geo_file, indent=4)
    simple_geo_file.close()
    os.chmod(simple_geo_path, 0664)

    return 0


# -------------------------------------------------------------------------
# temporal functions

def run_file_mask(fmask, fname, fbase=0):
    """extract temporal data from file name
    """

    if fbase and fname.startswith(fbase):
        fname = fname[fname.index(fbase) + len(fbase) + 1:]

    output = {
        "year": "".join([x for x,y in zip(fname, fmask) if y == 'Y' and x.isdigit()]),
        "month": "".join([x for x,y in zip(fname, fmask) if y == 'M' and x.isdigit()]),
        "day": "".join([x for x,y in zip(fname, fmask) if y == 'D' and x.isdigit()])
    }

    return output


def validate_date(date_obj):
    """validate a date object
    """

    # year is always required
    y = date_obj["year"]
    m = date_obj["month"]
    d = date_obj["day"]

    if y == "":
        return False, "No year found for data."

    # full 4 digit year required
    elif len(y) != 4:
        return False, "Invalid year."

    # months must always use 2 digits
    elif m != "" and len(m) != 2:
        return False, "Invalid month."

    # days of month (day when month is given) must always use 2 digits
    elif m != "" and d != "" and len(d) != 2:
        return False, "Invalid day of month."

    # days of year (day when month is not given) must always use 3 digits
    elif m == "" and d != "" and len(d) != 3:
        return False, "Invalid day of year."

    return True, None


# generate date range and date type from date object
def get_date_range(date_obj, drange=0):

    y = date_obj["year"]
    m = date_obj["month"]
    d = date_obj["day"]

    date_type = "None"

    # year, day of year (7)
    if m == "" and len(d) == 3:
        tmp_start = datetime.datetime(int(y), 1, 1) + datetime.timedelta(int(d)-1)
        tmp_end = tmp_start + relativedelta(days=drange)
        date_type = "day of year"

    # year, month, day (8)
    if m != "" and len(d) == 2:
        tmp_start = datetime.datetime(int(y), int(m), int(d))
        tmp_end = tmp_start + relativedelta(days=drange)
        date_type = "year month day"

    # year, month (6)
    if m != "" and d == "":
        tmp_start = datetime.datetime(int(y), int(m), 1)
        month_range = calendar.monthrange(int(y), int(m))[1]
        tmp_end = datetime.datetime(int(y), int(m), month_range)
        date_type = "year month"

    # year (4)
    if m == "" and d == "":
        tmp_start = datetime.datetime(int(y), 1, 1)
        tmp_end = datetime.datetime(int(y), 12, 31)
        date_type = "year"

    return (int(datetime.datetime.strftime(tmp_start, '%Y%m%d')),
            int(datetime.datetime.strftime(tmp_end, '%Y%m%d')),
            date_type)


