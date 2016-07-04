
import sys
import os
import json

import pandas as pd
import geopandas as gpd
from osgeo import gdal,ogr,osr
from shapely.geometry import Point

import datetime
import calendar
from dateutil.relativedelta import relativedelta


class ResourceTools():
    """Functions for working with dataset resources.

    Attributes:
        doc (Dict): x
        file_list (List): x
        temporal (Dict): x
        spatial (): x
        resources (List): x

    """
    def __init__(self):

        self.doc = {}

        self.file_list = []

        self.temporal = {
            "start": 0,
            "end": 0,
            "name": ""
        }

        self.spatial = ""

        self.resources = []


    def update_dp(self):
        self.doc["temporal"] = self.temporal
        self.doc["spatial"] = self.spatial
        self.doc["resources"] = self.resources


    # --------------------------------------------------
    # spatial functions


    # get bounding box
    # http://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
    # gets raw extent of raster
    def GetExtent(self, gt, cols, rows):
        ''' Return list of corner coordinates from a geotransform

            @type gt:   C{tuple/list}
            @param gt: geotransform
            @type cols:   C{int}
            @param cols: number of columns in the dataset
            @type rows:   C{int}
            @param rows: number of rows in the dataset
            @rtype:    C{[float,...,float]}
            @return:   coordinates of each corner
        '''

        ext=[]
        xarr=[0,cols]
        yarr=[0,rows]

        for px in xarr:
            for py in yarr:
                x = gt[0] + (px*gt[1]) + (py*gt[2])
                y = gt[3] + (px*gt[4]) + (py*gt[5])
                ext.append([x,y])
                # print x,y

            yarr.reverse()

        return ext


    # reprojects raster
    # def ReprojectCoords(self, coords, src_srs, tgt_srs):
    #     ''' Reproject a list of x,y coordinates.

    #         @type geom:     C{tuple/list}
    #         @param geom:    List of [[x,y],...[x,y]] coordinates
    #         @type src_srs:  C{osr.SpatialReference}
    #         @param src_srs: OSR SpatialReference object
    #         @type tgt_srs:  C{osr.SpatialReference}
    #         @param tgt_srs: OSR SpatialReference object
    #         @rtype:         C{tuple/list}
    #         @return:        List of transformed [[x,y],...[x,y]] coordinates
    #     '''

    #     print src_srs

    #     trans_coords=[]
    #     transform = osr.CoordinateTransformation(src_srs, tgt_srs)

    #     for x,y in coords:
    #         x,y,z = transform.TransformPoint(x,y)
    #         trans_coords.append([x,y])

    #     return trans_coords


    # acceptas new and old envelope
    # updates old envelope is new envelope bounds exceed old envelope bounds
    # envelope format [xmin, xmax, ymin, ymax]
    def check_envelope(self, new, old):
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


    # gets bounds of specified raster file
    def raster_envelope(self, path):
        ds = gdal.Open(path)

        gt = ds.GetGeoTransform()
        cols = ds.RasterXSize
        rows = ds.RasterYSize
        ext = self.GetExtent(gt,cols,rows)

        # src_srs = osr.SpatialReference()
        # src_srs.ImportFromWkt(ds.GetProjection())

        # tgt_srs = osr.SpatialReference()
        # tgt_srs.ImportFromEPSG(4326)
        # # tgt_srs = src_srs.CloneGeogCS()

        # geo_ext = self.ReprojectCoords(ext, src_srs, tgt_srs)
        # # geo_ext = [[-155,50],[-155,-30],[22,-30],[22,50]]

        geo_ext = ext

        return geo_ext


    # gets bounds of specified vector file
    # iterates over polygons and generates envelope using check_envelope function
    def vector_envelope(self, path):
        ds = ogr.Open(path)
        lyr_name = path[path.rindex('/')+1:path.rindex('.')]
        # lyr = ds.GetLayerByName(lyr_name)
        lyr = ds.GetLayer(0)
        env = []

        for feat in lyr:
            temp_env = feat.GetGeometryRef().GetEnvelope()
            env = self.check_envelope(temp_env, env)
            # print temp_env

        # env = [xmin, xmax, ymin, ymax]
        geo_ext = [[env[0],env[3]], [env[0],env[2]], [env[1],env[2]], [env[1],env[3]]]
        # print "final env:",env
        # print "bbox:",geo_ext

        return geo_ext


    # def vector_list(self, list=[]):
    #     env = []
    #     for file in list:
    #         f_env = vectory_envelope(file)
    #         env = self.check_envelope(f_env, env)

    #     geo_ext = [[env[0],env[3]], [env[0],env[2]], [env[1],env[2]], [env[1],env[3]]]
    #     return geo_ext


    # return a point given a pandas row (or any object) which
    #   includes longitude and latitude
    # return "None" if valid lon,lat not found
    def point_gen(self, item):

        try:
            lon = float(item['longitude'])
            lat = float(item['latitude'])
            return Point(lon, lat)
        except:
            return "None"


    def release_envelope(self, path):

        if not os.path.isfile(path):
            quit("Locations table could not be found.")

        try:
            df = pd.read_csv(path, sep=",", quotechar='\"')
        except:
            quit("Error reading locations table.")


        df['geometry'] = df.apply(self.point_gen, axis=1)

        gdf = gpd.GeoDataFrame(df.loc[df.geometry != "None"])

        env = gdf.total_bounds

        # env = (minx, miny, maxx, maxy)
        geo_ext = [[env[0],env[3]], [env[0],env[1]], [env[2],env[1]], [env[2],env[3]]]

        return geo_ext


    # adds unique id field (ad_id) and outputs geojson (serves as shp to geojson converter)
    def add_ad_id(self, path):

        try:
            geo_df = gpd.GeoDataFrame.from_file(path)
            geo_df["asdf_id"] = range(len(geo_df))

            geo_json = geo_df.to_json()
            geo_file = open(os.path.splitext(path)[0] + ".geojson", "w")
            json.dump(json.loads(geo_json), geo_file, indent = 4)

            # create simplified geojson for use with leaflet web map
            geo_df['geometry'] = geo_df['geometry'].simplify(0.01)
            json.dump(json.loads(geo_df.to_json()), open(os.path.dirname(path)+"/simplified.geojson", "w"), indent=4)


            return 0

        except:
            return (1, "error generating geojson with ad_id")


    # -------------------------------------------------------------------------
    # temporal functions


    def run_file_mask(self, fmask, fname, fbase=0):
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


    def validate_date(self, date_obj):
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
    def get_date_range(self, date_obj, drange=0):
  
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


