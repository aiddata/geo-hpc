
from __future__ import print_function

import sys
import math

import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon, Point, shape, box

import pyproj
from functools import partial
from shapely.ops import transform

from shapely.prepared import prep


# functions for generating user prompts
class CoreMSR():
    """class summary...

    class documentation...

    Attributes:
        pixel_size (float): pixel size
        psi (float): pixel size inverse

        nodata (int): nodata value for output raster
        aid_field (str): field name (from csv files) for aid values
        is_geocoded (str): field name (from csv files) identifying if project is geocoded (1/0)
        only_geocoded (bool): when True, only use geocoded data

        code_field_1 (str): primary field name associated with values in lookup dict
        code_field_2 (str): secondary field name associated with values in lookup dict
        not_geocoded (str): agg_type definition for non geocoded projects. can either allocated at country level (use: "country") or ignored (use: "None")

        agg_types ([str, ...]): aggregation types used in lookup dict
        lookup (dict):  precision and feature code values (uses default if feature code not listed)
                        buffer values in meters
                        for adm0 / country boundary  make sure to use type "country" instead of "adm" with data "0"

        adm_shps : list containing adm shape lists for each adm level (indexed by adm level #)
        adm0 : shapely shape representing coarsest spatial unit
        prep_adm0 : prepared shapely shape of adm0 for faster spatial functions

        All attributes except adm0 have default values built into __init__.
        Any attributes may be updated but be sure to use setter functions when available as they will 
        verify the new value. Read comments/documentation before changing attribute values for which setter functions are not 
        available to verify new values follow standards or available acceptable values.
    """

    def __init__(self):


        self.pixel_size = 0.05

        self.psi = 1/pixel_size


        # --------------------------------------------------
        # vars to potentially be added as inputs
        # (not used by functions)

        self.nodata = -9999

        self.aid_field = "total_commitments"

        self.is_geocoded = "is_geocoded"

        self.only_geocoded = False


        # --------------------------------------------------
        # vars that may be added as some type of input
        # (used by functions)


        self.code_field_1 = "precision_code"
        self.code_field_2 = "location_type_code"

        
        self.not_geocoded = "country"

        if self.only_geocoded:
            self.not_geocoded = "None"


        self.agg_types = ["point", "buffer", "adm"]


        self.lookup = {
            "1": {
                "default": {"type": "point", "data": 0}
            },
            "2": {
                "default": {"type": "buffer", "data": 25000}
            },
            "3": {
                "default": {"type": "adm", "data": "2"}
            },
            "4": {
                "default": {"type": "adm", "data": "1"}
            },
            "5": {
                "default": {"type": "buffer", "data": 25000}
            },
            "6": {
                "default": {"type": "country", "data": 0}
            },
            "7": {
                "default": {"type": "country", "data": 0}
            },
            "8": {
                "default": {"type": "country", "data": 0}
            }
        }


        self.adm_shps = 0
        self.adm0 = 0
        self.prep_adm0 = 0


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
            sys.exit("pixel size given could not be converted to float: "+str(value))

        # check for valid pixel size
        # examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
        if (1/value) != int(1/value):
            sys.exit("invalid pixel size: "+str(value))

        self.pixel_size = value
        self.psi = 1/value


    def set_adm0(self, shp):
        """Set value of adm0 and prep_adm0 attributes

        Args:
            shape: shapely shape

        Will exit script if shape is not a valid Polygon or MultiPolygon
        """
        if isinstance(shape(shp), Polygon) or isinstance(shape(shp), MultiPolygon):
            self.adm0 = shape(shp)
            self.prep_adm0 = prep(self.adm0)
        else:
            sys.exit("invalid adm0 given")


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
                return pd.read_csv(path, sep='\t', quotechar='\"', na_values='', keep_default_na=False)
            except:
                return 'unable to open file (' + str(path) + ')'
        elif path.endswith('.csv'):
            try:
                return pd.read_csv(path, quotechar='\"', na_values='', keep_default_na=False)
            except:
                return 'unable to open file (' + str(path) + ')'
        else:
            return 'file extension not recognized (' + str(path) + ')'


    # requires a field name to merge on and list of required fields
    def merge_data(self, path, merge_id, field_ids=None, only_geo=False):
        """Retrieves and merges data from project and location tables.

        Args:
            path (str): absolute path to directory where project and location tables exist [required]
            merge_id (str): field to merge on [required]
            field_ids ([str, ...]): list of fields to verify exist in merged dataframe [optional, default None]
            only_geo (bool): whether to use inner merge (true) or left merge (false) [optional, default False]
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
                print(amp)

            if type(loc) == str:
                print(loc)

            sys.exit("merge_data - error opening files")


        # make sure merge id field exists in both files
        if not merge_id in amp or not merge_id in loc:
            sys.exit("merge_data - merge field not found in amp or loc files")

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
            sys.exit("merge_data - latitude and longitude fields not found")

        # make sure option fields are present
        if field_ids == None:
            field_ids = []

        for field_id in field_ids:
            if not field_id in tmp_merged:
                sys.exit("merge_data - required code field not found")

        return tmp_merged


    def get_geom_type(self, is_geo, code_1, code_2):
        """Get geometry type based on lookup table.

        Args:
            is_geo : if project has geometry
            code_1 : precisions code
            code_2 : location type
        Returns:
            geometry type
        """
        try:
            is_geo = int(is_geo)
            code_1 = str(int(code_1))
            code_2 = str(code_2)

            if is_geo == 1:
                if code_1 in self.lookup:
                    if code_2 in self.lookup[code_1]:
                        tmp_type = self.lookup[code_1][code_2]["type"]
                        return tmp_type
                    else:
                        tmp_type = self.lookup[code_1]["default"]["type"]
                        return tmp_type
                else:
                    print("lookup code_1 not recognized: " + code_1)
                    return "None"

            elif is_geo == 0:
                return self.not_geocoded

            else:
                print("is_geocoded integer code not recognized: " + str(is_geo))
                return "None"

        except:
            return self.not_geocoded


    def get_shape_within(self, shp, polys):
        """Find shape in set of shapes which another given shape is within.

        Args:
            shp (shape): shape object
            polys ([shape, ...]): list of shapes
        Returns:
            If shape is found in polys which shp is within, return shape.
            If not shape is found, return 0.
        """
        if not hasattr(shp, 'geom_type'):
            sys.exit("CoreMSR [get_shape_within] : invalid shp given")

        if not isinstance(polys, list):
            sys.exit("CoreMSR [get_shape_within] : invalid polys given")

        for poly in polys:
            tmp_poly = shape(poly)
            if shp.within(tmp_poly):
                return tmp_poly

        return 0


    def is_in_country(self, shp):
        """Check if arbitrary polygon is within country (adm0) polygon.

        Args:
            shp (shape):
        Returns:
            Bool whether shp is in adm0 shape.

        Depends on prep_adm0 being defined in environment.
        """
        if not hasattr(shp, 'geom_type'):
            sys.exit("CoreMSR [is_in_country] : invalid shp given")

        if not isinstance(self.prep_adm0, type(prep(Point(0,0)))):
            sys.exit("CoreMSR [is_in_country] : invalid prep_adm0 found")

        return self.prep_adm0.contains(shp)


    # build geometry for point based on code
    # depends on lookup and adm0
    def get_geom(self, code_1, code_2, lon, lat):
        """Get geometry for point using lookup table.

        Args:
            code_1 (str) : primary location identifier (eg: precision code)
            code_2 (str) : secondary location identifier (eg: location type code)
            lon : longitude
            lat : latitude
        Returns:
            shape for geometry identified by lookup table 
            or 0 for geometry that is outside adm0 or could not be identified
        """
        tmp_pnt = Point(lon, lat)

        if not self.is_in_country(tmp_pnt):
            print("point not in country")
            return 0

        else:
            if code_2 in lookup[code_1]:
                tmp_lookup = lookup[code_1][code_2]
            else:
                tmp_lookup = lookup[code_1]["default"]

            # print(tmp_lookup["type"])

            if tmp_lookup["type"] == "point":
                return tmp_pnt

            elif tmp_lookup["type"] == "buffer":
                try:
                    # get buffer size (meters)
                    tmp_int = float(tmp_lookup["data"])

                    # reproject point
                    proj_utm = pyproj.Proj('+proj=utm +zone=45 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ')
                    proj_wgs = pyproj.Proj(init="epsg:4326")

                    utm_pnt_raw = pyproj.transform(proj_wgs, proj_utm, tmp_pnt.x, tmp_pnt.y)
                    utm_pnt_act = Point(utm_pnt_raw)

                    # create buffer in meters
                    utm_buffer = utm_pnt_act.buffer(tmp_int)

                    # reproject back
                    buffer_proj = partial(pyproj.transform, proj_utm, proj_wgs)
                    tmp_buffer = transform(buffer_proj, utm_buffer)

                    # clip buffer if it extends outside country
                    if is_in_country(tmp_buffer):
                        return tmp_buffer
                    else:
                        return tmp_buffer.intersection(adm0)

                except:
                    print("buffer value could not be converted to float")
                    return 0

            elif tmp_lookup["type"] == "adm":
                try:
                    tmp_int = int(tmp_lookup["data"])
                    return self.get_shape_within(tmp_pnt, self.adm_shps[tmp_int])

                except:
                    print("adm value could not be converted to int")
                    return 0

            else:
                print("geom object type not recognized")
                return 0


    def get_geom_val(self, agg_type, code_1, code_2, lon, lat):
        """Manage finding geometry for point based on geometry type.

        Args:
            agg_type (str) : geometry type
            code_1 (str) : primary location identifier (eg: precision code)
            code_2 (str) : secondary location identifier (eg: location type code)
            lon : longitude
            lat : latitude
        Returns:
            geometry (shape) or "None"

        Method for finding actual geometry varies by geometry type.
        For point, buffer and adm types the lookup table is needed so the get_geom function is called.
        Country types can simply return the adm0 attribute.
        Unrecognized types return None.
        """
        if agg_type in self.agg_types:

            tmp_geom = self.get_geom(code_1, code_2, lon, lat)

            if tmp_geom != 0:
                return tmp_geom

            return "None"

        elif agg_type == "country":

            return self.adm0

        else:
            print("agg_type not recognized: " + agg_type)
            return "None"


    def adjust_aid(self, raw_aid, project_sectors_string, project_donors_string, filter_sectors_list, filter_donors_list):
        """Adjusts given aid value based on percentage of sectors/donors in filter vs project.

        Args:
            raw_aid (float): given aid value
            project_sectors_string (str): pipe (|) separated string of sectors from project table
            project_donors_string (str): pipe (|) separated string of donors from project table
            filter_sectors_list ([str, ...]): list of donors selected via filter
            filter_donors_list ([str, ...]): list of donors selected via filter
        Returns:
            adjusted aid value (float)

        Aid value is adjust based on the ratio of donors and sectors selected via filter when
        compared to the total number of (distinct) donors and sectors associated with a project.
        """
        project_sectors_list = project_sectors_string.split('|')
        project_donors_list = project_donors_string.split('|')

        if filter_sectors_list == ['All']:
            sectors_match = project_sectors_list
        else:
            sectors_match = [match for match in project_sectors_list if match in filter_sectors_list]

        if filter_donors_list == ['All']:
            donors_match = project_donors_list
        else:  
            donors_match = [match for match in project_donors_list if match in filter_donors_list]

        ratio = float(len(sectors_match) * len(donors_match)) / float(len(project_sectors_list) * len(project_donors_list))

        # remove duplicates? - could be duplicates from project strings
        # ratio = (len(set(sectors_match)) * len(set(donors_match))) / (len(set(project_sectors_list)) * len(set(project_donors_list)))

        adjusted_aid = ratio * float(raw_aid)

        return adjusted_aid


    def geom_to_grid_colrows(self, geom, step, rounded=True, no_multi=False):
        """Generate column/row lists for grid based on geometry.

        Args:
            geom (shape): geometry to be used (must be shape or be able to be converted to shape)
            step (float): grid pixel size
            rounded (bool): flag to round output column/row values
            no_multi (bool): flag to allow using multipolygons
        Returns:
            for valid geom: tuple of lists for columns (longitude) and rows (latitude) of grid 
            (columns, rows)
            for invalid geom: 1
        """
        # check if geom is polygon
        if geom != Polygon:
            try:
                # make polygon if needed and possible
                geom = shape(geom)

                # if no_multi == True and geom != Polygon:
                #     return 2

            except:
                # cannot convert geom to polygon
                return 1


        # poly grid pixel size and poly grid pixel size inverse
        # poly grid pixel size is 1 order of magnitude higher resolution than output pixel_size
        tmp_pixel_size = float(step)
        tmp_psi = 1/tmp_pixel_size

        (tmp_minx, tmp_miny, tmp_maxx, tmp_maxy) = geom.bounds

        (tmp_minx, tmp_miny, tmp_maxx, tmp_maxy) = (math.floor(tmp_minx*tmp_psi)/tmp_psi, math.floor(tmp_miny*tmp_psi)/tmp_psi, math.ceil(tmp_maxx*tmp_psi)/tmp_psi, math.ceil(tmp_maxy*tmp_psi)/tmp_psi)

        tmp_cols = np.arange(tmp_minx, tmp_maxx+tmp_pixel_size*0.5, tmp_pixel_size)
        tmp_rows = np.arange(tmp_miny, tmp_maxy+tmp_pixel_size*0.5, tmp_pixel_size)

        if rounded == True:
            tmp_sig = 10 ** len(str(tmp_pixel_size)[str(tmp_pixel_size).index('.')+1:])

            tmp_cols = [round(i * tmp_sig) / tmp_sig for i in tmp_cols]
            tmp_rows = [round(i * tmp_sig) / tmp_sig for i in tmp_rows]


        return tmp_cols, tmp_rows


    def positive_zero(val):
        """Convert "negative" zero values to +0.0

        Args:
            val: number (should be float, but not checked)
        Returns:
            If val equals zero return +0.0 to make sure val was not a "negative" zero,
            otherwise return val.

        Needed as a result of how binary floating point works.
        """
        if val == 0:
            return +0.0
        else:
            return val

