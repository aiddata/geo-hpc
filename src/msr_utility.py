
import sys
import math
import numpy as np
import pandas as pd
import geopandas as gpd
import utm
import pyproj
import itertools
import re

from functools import partial
from collections import OrderedDict
from shapely.geometry import MultiPolygon, Polygon, Point, shape, box
from shapely.ops import transform
from shapely.prepared import prep

import rasterio
from rasterio import features
from affine import Affine
import warnings


class MasterStack:
    """Manage stack of grid arrays produced by workers

    Attributes:
        all_mean_surf (list): array of grid arrays
    """
    def __init__(self):
        self.all_mean_surf = []


    def get_stack_size(self):
        """Get size of all_mean_surf

        Returns:
            size (int)
        """
        return len(self.all_mean_surf)


    def append_stack(self, data):
        """Append new data to all_mean_surf

        Args:
            data: new data for all_mean_surf
        """
        self.all_mean_surf.append(data)


    def get_stack_sum(self):
        """Create stack from all_mean_surf and sums

        Returns:
            sum of all_mean_surf stack
        """
        stack_mean_surf = np.vstack(self.all_mean_surf)
        sum_mean_surf = np.sum(stack_mean_surf, axis=0)
        return sum_mean_surf


    def reduce_stack(self):
        """Reduce items in all_mean_surf by summing

        Used to reduce memory footprint
        """
        self.all_mean_surf = [self.get_stack_sum()]



class CoreMSR():
    """Core variables & functions used by mean-surface-rasters runscript.

    Attributes:
        pixel_size (float): pixel size
        psi (float): pixel size inverse

        nodata (int): nodata value for output raster
        aid_field (str): field name (from csv files) for aid values
        is_geocoded (str): field name (from csv files) identifying if
                           project is geocoded (1/0)
        only_geocoded (bool): when True, only use geocoded data

        code_field_1 (str): primary field name associated with
                            values in lookup dict
        code_field_2 (str): secondary field name associated with
                            values in lookup dict
        not_geocoded (str): geom_type definition for non geocoded
                            projects. can either allocated at country
                            level (use: "country") or
                            ignored (use: "None")

        geom_types (List[str]): aggregation types used in lookup dict
        lookup (dict):  precision and feature code values (uses default
                        if feature code not listed)
                        - buffer values in meters
                        - for adm0 / country boundary  make sure to use
                          type "country" instead of "adm" with data "0"

        adm_shps : list containing adm shape lists for each adm level
                   (indexed by adm level #)
        adm0 : shapely shape representing coarsest spatial unit
        prep_adm0 : prepared shapely shape of adm0 for
                    faster spatial functions

        All attributes except adm0 have default values built
        into __init__.

        Any attributes may be updated but be sure to use setter
        functions when available as they will verify the new value.
        Read comments/documentation before changing attribute values
        for which setter functions are not available to verify new
        values follow standards or available acceptable values.
    """
    def __init__(self):


        # --------------------------------------------------
        # current input vars (and direct derivations)

        self.pixel_size = 0.05

        self.psi = 1/self.pixel_size


        # --------------------------------------------------
        # vars to potentially be added as inputs
        # (not used by functions)

        self.nodata = -9999

        self.aid_field = "total_commitments"

        self.is_geocoded = "is_geocoded"

        self.only_geocoded = True


        # --------------------------------------------------
        # vars that may be added as some type of input
        # (used by functions)

        self.not_geocoded = "country"

        if self.only_geocoded:
            self.not_geocoded = "None"


        self.geom_types = ["point", "buffer", "adm"]


        # self.code_field_1 = "location_class"
        # self.code_field_3 = "geographic_exactness"

        # fields are currently bugged and switched
        self.code_field_1 = "geographic_exactness"
        self.code_field_3 = "location_class"


        self.code_field_2 = "location_type_code"


        # based on IATI 2.01 codes
        # location class code > location type code > geographic exactness code
        self.lookup = {
            "1": {
                "default": {
                    "1": {"type": "buffer", "data": 25000},
                    "2": {"type": "country", "data": 0}
                },
                "ADM1": {
                    "1": {"type": "adm", "data": "1"},
                    "2": {"type": "country", "data": 0}
                },
                "ADM2": {
                    "1": {"type": "adm", "data": "2"},
                    "2": {"type": "country", "data": 0}
                }#,
                # "ADM3": {
                #     "1": {"type": "adm", "data": "3"},
                #     "2": {"type": "adm", "data": "0"}
                # },
                # "ADM4": {
                #     "1": {"type": "adm", "data": "4"},
                #     "2": {"type": "adm", "data": "0"}
                # },
                # "ADM5": {
                #     "1": {"type": "adm", "data": "5"},
                #     "2": {"type": "adm", "data": "0"}
                # }
            },
            "2": {
                "default": {
                    "1":  {"type": "buffer", "data": 1000},
                    "2":  {"type": "buffer", "data": 25000}
                }
            },
            "3": {
                "default": {
                    "1":  {"type": "buffer", "data": 1000},
                    "2":  {"type": "buffer", "data": 25000}
                }
            },
            "4": {
                "default": {
                    "1":  {"type": "buffer", "data": 25000},
                    "2":  {"type": "buffer", "data": 50000}
                }
            }
        }


        # --------------------------------------------------

        self.times = OrderedDict()
        self.durations = OrderedDict()

        self.adm_shps = 0
        self.adm0 = 0
        self.prep_adm0 = 0

        self.bounds = None
        self.shape = None
        self.affine = None
        self.topleft = None


    def set_pixel_size(self, value):
        """Set pixel size.

        Args:
            value (float): new pixel size (max value of 1, no min)

        Setter will validate pixel size and set attribute.
        Also calculates psi (pixel size inverse) and sets attribute.
        """
        try:
            value = float(value)
        except:
            raise Exception("pixel size given could not be converted to "
                            "float ({0})".format(value))

        # check for valid pixel size
        # examples of valid pixel sizes:
        # 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
        if (1/value) != int(1/value):
            raise Exception("invalid pixel size: "+str(value))

        self.pixel_size = value
        self.psi = 1/value


    def set_adm0(self, shp):
        """Set value of adm0 and prep_adm0 attributes

        Args:
            shape: shapely shape

        Will exit script if shape is not a valid Polygon or
        MultiPolygon
        """
        if isinstance(shape(shp), (Polygon, MultiPolygon)):
            self.adm0 = shape(shp)
            self.prep_adm0 = prep(self.adm0)
        else:
            raise Exception("invalid adm0 given")


    def load_csv(self, path):
        """Read csv from file.

        Args:
            path (str): absolute path for file
        Returns:
            Pandas dataframe if file is loaded sucessfully,
            string if errors are encountered

        Check file extension and if valid,
        read the csv using the relevant sep field.
        """
        if path.endswith('.tsv'):
            try:
                return pd.read_csv(path,
                                   sep='\t', quotechar='\"',
                                   na_values='', keep_default_na=False,
                                   encoding='utf-8')
            except:
                return 'unable to open file (' + str(path) + ')'
        elif path.endswith('.csv'):
            try:
                return pd.read_csv(path,
                                   quotechar='\"', na_values='',
                                   keep_default_na=False, encoding='utf-8')
            except:
                return 'unable to open file (' + str(path) + ')'
        else:
            return 'file extension not recognized (' + str(path) + ')'


    # requires a field name to merge on and list of required fields
    def merge_data(self, path, merge_id, field_ids=None, only_geo=False):
        """Retrieves and merges data from project and location tables.

        Args:
            path (str): absolute path to directory where project and
                        location tables exist [required]
            merge_id (str): field to merge on [required]
            field_ids (List[str]): list of fields to verify exist in
                                   merged dataframe
                                   [optional, default None]
            only_geo (bool): whether to use inner merge (true) or
                             left merge (false) [optional, default False]
        Returns:
            merged dataframe containing project and location data

        Function will terminate script if errors are encountered.
        """
        amp_path = path+"/projects.csv"
        loc_path = path+"/locations.csv"

        # make sure files exist
        #

        # read input csv files into memory
        amp = self.load_csv(amp_path)
        loc = self.load_csv(loc_path)

        # make sure there were no issues opening files
        if type(amp) == str or type(loc) == str:
            if type(amp) == str:
                print amp

            if type(loc) == str:
                print loc

            raise Exception("merge_data - error opening files")


        # make sure merge id field exists in both files
        if not merge_id in amp or not merge_id in loc:
            raise Exception("merge_data - merge field not found in amp or "
                            "loc files")

        # convert merge id field to string to prevent potential type issues
        amp[merge_id] = amp[merge_id].astype(str)
        loc[merge_id] = loc[merge_id].astype(str)

        # merge amp and location files on merge_id
        if only_geo:
            tmp_merged = amp.merge(loc, on=merge_id)
        else:
            tmp_merged = amp.merge(loc, on=merge_id, how="left")

        # make sure merge dataframe has longitude and latitude fields
        # so it can be converted to geodataframe later
        if not "longitude" in tmp_merged or not "latitude" in tmp_merged:
            raise Exception("merge_data - latitude and longitude fields "
                            "not found")

        # make sure option fields are present
        if field_ids == None:
            field_ids = []

        for field_id in field_ids:
            if not field_id in tmp_merged:
                raise Exception("merge_data - required code field not found")

        return tmp_merged



    def process_data(self, data_directory, request_object):

        df_merged = self.prep_data(data_directory)

        filters = request_object['options']['filters']

        df_filtered = self.filter_data(df_merged, filters)

        df_adjusted = self.adjust_aid(df_filtered, filters)

        df_geom = self.assign_geom_type(df_adjusted)

        # df_geom['index'] = df_geom['project_location_id']
        df_geom['task_ids'] = range(0, len(df_geom))
        df_geom['index'] = range(0, len(df_geom))
        df_geom = df_geom.set_index('index')

        return df_geom


    def prep_data(self, data_directory):

        df_merged = self.merge_data(
            data_directory, "project_id", (self.code_field_1,
            self.code_field_2, self.code_field_3, "project_location_id"),
            self.only_geocoded)


        # get location count for each project
        df_merged['ones'] = (pd.Series(np.ones(len(df_merged)))).values

        # get project location count
        grouped_location_count = df_merged.groupby('project_id')['ones'].sum()


        # create new empty dataframe
        df_location_count = pd.DataFrame()

        # add location count series to dataframe
        df_location_count['location_count'] = grouped_location_count

        # add project_id field
        df_location_count['project_id'] = df_location_count.index

        # merge location count back into data
        df_merged = df_merged.merge(df_location_count, on='project_id')

        # aid field value split evenly across
        # all project locations based on location count
        df_merged[self.aid_field].fillna(0, inplace=True)
        df_merged['split_dollars_pp'] = (
            df_merged[self.aid_field] / df_merged.location_count)

        return df_merged


    def filter_data(self, df_merged, filters):

        df_filtered = df_merged.copy(deep=True)

        for filter_field in filters.keys():

            tmp_filter = filters[filter_field]

            if tmp_filter and 'All' not in tmp_filter:

                if filter_field == "years":

                    year_list = tmp_filter
                    # need to add year filter to check if year is
                    # between transaction_start_year and
                    # transaction_end_year
                    df_filtered = df_filtered.loc[
                        df_filtered.apply(
                            lambda z: any(
                                int(y) >= int(z.transactions_start_year) and
                                int(y) <= int(z.transactions_end_year)
                                for y in year_list),
                            axis=1)
                    ].copy(deep=True)

                else:
                    df_filtered = df_filtered.loc[
                        df_filtered[filter_field].str.contains(
                            '|'.join([re.escape(i) for i in tmp_filter]))
                    ].copy(deep=True)


        return df_filtered


    def adjust_aid(self, df_filtered, filters):

        df_adjusted = df_filtered.copy(deep=True)

        # adjust aid based on ratio of sectors/donors in
        # filter to all sectors/donors listed for project

        if not 'ad_sector_names' in filters.keys():
            sector_split_list = []
        else:
            sector_split_list = filters['ad_sector_names']

        if not 'donors' in filters.keys():
            donor_split_list = []
        else:
            donor_split_list = filters['donors']

        df_adjusted['adjusted_aid'] = df_adjusted.apply(
            lambda z: self.calc_adjusted_aid(
                z.split_dollars_pp, z.ad_sector_names, z.donors,
                sector_split_list, donor_split_list), axis=1)

        return df_adjusted


    def assign_geom_type(self, df_adjusted):

        df_geom = df_adjusted.copy(deep=True)

        # add geom columns
        df_geom["geom_type"] = pd.Series(["None"] * len(df_geom))

        df_geom.geom_type = df_geom.apply(lambda x: self.get_geom_type(
            x[self.is_geocoded], x[self.code_field_1], x[self.code_field_2],
            x[self.code_field_3]), axis=1)

        return df_geom


    def get_geom_type(self, is_geo, code_1, code_2, code_3):
        """Get geometry type based on lookup table.

        Args:
            is_geo : if project has geometry
            code_1 (str) : location class code
            code_2 (str) : location type code
            code_3 (str) : geographic exactness code
        Returns:
            geometry type
        """
        is_geo = str(int(is_geo))

        if is_geo == "1":

            code_1 = str(int(code_1))
            code_2 = str(code_2)
            code_3 = str(int(code_3))

            if code_1 not in self.lookup:
                print "lookup code_1 not recognized: " + code_1
                return "None"

            if code_2 not in self.lookup[code_1]:
                code_2 = "default"

            if code_3 not in self.lookup[code_1][code_2]:
                print "lookup code_3 not recognized: " + code_3
                return "None"

            tmp_type = self.lookup[code_1][code_2][code_3]["type"]
            return tmp_type

        elif is_geo == "0":
            return self.not_geocoded

        else:
            print "is_geocoded integer code not recognized: "+str(is_geo)
            return "None"



    def get_shape_within(self, shp, polys):
        """Find shape in set of shapes which another given shape is within.

        Args:
            shp (shape): shape object
            polys (List[shape]): list of shapes
        Returns:
            If shape is found in polys which shp is within, return shape.
            If not shape is found, return None.
        """
        if not hasattr(shp, 'geom_type'):
            raise Exception("CoreMSR [get_shape_within] : invalid shp given")

        if not isinstance(polys, list):
            raise Exception("CoreMSR [get_shape_within] : invalid polys given")

        for poly in polys:
            tmp_poly = shape(poly)
            if shp.within(tmp_poly):
                return tmp_poly

        print "CoreMSR [get_shape_within] : shp not within any given poly"

        return "None"


    def is_in_country(self, shp):
        """Check if arbitrary polygon is within country (adm0) polygon.

        Args:
            shp (shape):
        Returns:
            Bool whether shp is in adm0 shape.

        Depends on prep_adm0 being defined in environment.
        """
        if not hasattr(shp, 'geom_type'):
            raise Exception("CoreMSR [is_in_country] : invalid shp given")

        if not isinstance(self.prep_adm0, type(prep(Point(0,0)))):
            raise Exception("CoreMSR [is_in_country] : invalid prep_adm0 "
                            "found")

        return self.prep_adm0.contains(shp)


    # build geometry for point based on code
    # depends on lookup and adm0
    def get_geom(self, code_1, code_2, code_3, lon, lat):
        """Get geometry for point using lookup table.

        Args:
            code_1 (str) : location class code
            code_2 (str) : location type code
            code_3 (str) : geographic exactness code
            lon : longitude
            lat : latitude
        Returns:
            shape for geometry identified by lookup table
            or 0 for geometry that is outside adm0 or could not be identified
        """
        tmp_pnt = Point(lon, lat)

        if not self.is_in_country(tmp_pnt):
            print "point not in country " + str(tmp_pnt)
            return "None"

        else:
            if code_2 not in self.lookup[code_1]:
                code_2 = "default"

            tmp_lookup = self.lookup[code_1][code_2][code_3]

            # print tmp_lookup["type"]

            if tmp_lookup["type"] == "point":
                return tmp_pnt

            elif tmp_lookup["type"] == "buffer":
                try:
                    # get buffer size (meters)
                    tmp_int = float(tmp_lookup["data"])
                except:
                    print "buffer value could not be converted to float"
                    raise

                try:
                    tmp_utm_info = utm.from_latlon(lat, lon)
                    tmp_utm_zone = str(tmp_utm_info[2]) + str(tmp_utm_info[3])

                    # reproject point
                    proj_utm = pyproj.Proj("+proj=utm +zone="
                        + str(tmp_utm_zone)
                        + " +ellps=WGS84 +datum=WGS84 +units=m +no_defs ")
                    proj_wgs = pyproj.Proj(init="epsg:4326")
                except:
                    print ("error initializing projs "
                           "(utm: {0})").format(tmp_utm_zone)
                    raise

                try:
                    utm_pnt_raw = pyproj.transform(proj_wgs, proj_utm,
                                                   tmp_pnt.x, tmp_pnt.y)
                    utm_pnt_act = Point(utm_pnt_raw)

                    # create buffer in meters
                    utm_buffer = utm_pnt_act.buffer(tmp_int)

                    # reproject back
                    buffer_proj = partial(pyproj.transform,
                                          proj_utm, proj_wgs)
                    tmp_buffer = transform(buffer_proj, utm_buffer)

                    # clip buffer if it extends outside country
                    if self.is_in_country(tmp_buffer):
                        return tmp_buffer
                    # elif tmp_buffer.intersects(self.adm0):
                        # return tmp_buffer.intersection(self.adm0)
                    else:
                        return tmp_buffer.intersection(self.adm0)
                        # return 0

                except:
                    print "error applying projs"
                    raise

            elif tmp_lookup["type"] == "adm":
                try:
                    tmp_int = int(tmp_lookup["data"])
                    return self.get_shape_within(tmp_pnt,
                                                 self.adm_shps[tmp_int])

                except:
                    print "adm value could not be converted to int"
                    raise

            else:
                print "geom object type not recognized"
                return "None"


    def get_geom_val(self, geom_type, code_1, code_2, code_3, lon, lat):
        """Manage finding geometry for point based on geometry type.

        Args:
            geom_type (str) : geometry type
            code_1 (str) : location class code
            code_2 (str) : location type code
            code_3 (str) : geographic exactness code
            lon : longitude
            lat : latitude
        Returns:
            geometry (shape) or "None"

        Method for finding actual geometry varies by geometry type.
        For point, buffer and adm types the lookup table is needed so the
        get_geom function is called.
        Country types can simply return the adm0 attribute.
        Unrecognized types return None.
        """
        if geom_type == "None":
            return "None"

        elif geom_type == "country":
            return self.adm0

        elif geom_type in self.geom_types:

            code_1 = str(int(code_1))
            code_2 = str(code_2)
            code_3 = str(int(code_3))

            tmp_geom = self.get_geom(code_1, code_2, code_3, lon, lat)

            return tmp_geom

        else:
            print "geom_type not recognized: " + geom_type
            return "None"


    def calc_adjusted_aid(self, raw_aid,
            project_sectors_string, project_donors_string,
            filter_sectors_list, filter_donors_list):
        """Adjusts given aid value based on filter.

        Args:
            raw_aid (float): given aid value
            project_sectors_string (str): pipe (|) separated string
                of sectors from project table
            project_donors_string (str): pipe (|) separated string of
                donors from project table
            filter_sectors_list (List[str]): list of donors selected
                via filter
            filter_donors_list (List[str]): list of donors selected
                via filter
        Returns:
            adjusted aid value (float)

        Aid value is adjusted based on the ratio of donors and
        sectors selected via filter when compared to the total
        number of (distinct) donors and sectors associated with
        a project.
        """
        project_sectors_list = project_sectors_string.split('|')
        project_donors_list = project_donors_string.split('|')

        if not filter_sectors_list or 'All' in filter_sectors_list:
            sectors_match = project_sectors_list
        else:
            sectors_match = [match for match in project_sectors_list
                             if match in filter_sectors_list]


        if not filter_donors_list or 'All' in filter_donors_list:
            donors_match = project_donors_list
        else:
            donors_match = [match for match in project_donors_list
                            if match in filter_donors_list]

        match = float(len(sectors_match) * len(donors_match))
        total = float(len(project_sectors_list) * len(project_donors_list))
        ratio = match / total

        # remove duplicates? - could be duplicates from project strings
        # match = (len(set(sectors_match)) * len(set(donors_match)))
        # total = (len(set(project_sectors_list)) *
        #            len(set(project_donors_list)))
        # ratio = match / total

        adjusted_aid = ratio * float(raw_aid)

        return adjusted_aid


    def set_grid_info(self, geom):
        """
        """
        if not hasattr(geom, 'geom_type'):
            raise Exception("CoreMSR [rasterize_geom] : invalid geom")

        pixel_size = self.pixel_size
        psi = 1 / pixel_size

        (minx, miny, maxx, maxy) = geom.bounds

        (minx, miny, maxx, maxy) = (
            np.floor(minx * psi) / psi,
            np.floor(miny * psi) / psi,
            np.ceil(maxx * psi) / psi,
            np.ceil(maxy * psi) / psi)

        top_left_lon = minx
        top_left_lat = maxy
        affine = Affine(pixel_size, 0, top_left_lon,
                        0, -pixel_size, top_left_lat)

        nrows = int(psi * (maxy - miny))
        ncols = int(psi * (maxx - minx))

        shape = (nrows, ncols)

        self.bounds = (minx, miny, maxx, maxy)
        self.affine = affine
        self.shape = shape
        self.topleft = (top_left_lon, top_left_lat)


    # https://stackoverflow.com/questions/8090229/
    #   resize-with-averaging-or-rebin-a-numpy-2d-array/8090605#8090605
    def rebin_sum(self, a, shape, dtype):
        sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
        return a.reshape(sh).sum(-1, dtype=dtype).sum(1, dtype=dtype)


    def rasterize_geom(self, geom, scale=1):
        """
        """
        if not hasattr(geom, 'geom_type'):
            raise Exception("CoreMSR [rasterize_geom] : invalid geom")

        try:
            fscale = float(scale)

            if fscale < 1:
                raise Exception("CoreMSR [rasterize_geom] : scale must be >=1")


            iscale = int(scale)

            if float(iscale) != fscale:
                warnings.warn("CoreMSR [rasterize_geom] : scale float ({0}) "
                              "converted to int ({1})").format(fscale, iscale)

        except:
            print "CoreMSR [rasterize_geom] : invalid scale type"
            raise


        scale = iscale

        if scale == 1:
            pixel_size = self.pixel_size
        elif scale % 2 != 0 or scale % 5 != 0:
            raise Exception("CoreMSR [rasterize_geom] : invalid scale")
        else:
            pixel_size = self.pixel_size / scale


        affine = Affine(pixel_size, 0, self.topleft[0],
                        0, -pixel_size, self.topleft[1])

        nrows = self.shape[0] * scale
        ncols = self.shape[1] * scale

        shape = (nrows, ncols)

        rasterized = features.rasterize(
            [(geom, 1)],
            out_shape=shape,
            transform=affine,
            fill=0,
            all_touched=False)

        if scale != 1:
            min_dtype = np.min_scalar_type(scale**2)
            rv_array = self.rebin_sum(rasterized, self.shape, min_dtype)
            return rv_array
        else:
            return rasterized


