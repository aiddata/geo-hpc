import os
import json
import re
from collections import OrderedDict
from log_prompt import prompts

import pymongo

p = prompts()


# validation functions, fields, etc.
class validate():

    def __init__(self):

        self.interface = False

        # store prompt inputs
        self.data = {}

        # base path
        self.dir_base = os.path.dirname(os.path.abspath(__file__))

        # available licenses
        self.licenses = json.load(open(self.dir_base + "/licenses.json", 'r'))

        # init file format
        self.file_format = ""

        # acceptable inputs for various fields (dataset types, vector formats, raster formats, etc.)
        self.types = {
            "licenses": self.licenses.keys(),
            "data": ['raster', 'polydata', 'document', 'point', 'multipoint', 'boundary'],
            "file_extensions": {
                "vector": ['geojson', 'shp'],
                "raster": ['tif', 'asc']
            },
            "extracts": ['mean', 'max', 'sum'],
            "group_class": ['actual', 'sub']
        }

        # error messages to go with validation functions
        self.error = {
            "is_dir": "not a directory",
            "name": "name exists or was rejected by user",
            "license_types": "at least one license id given not in list of valid license ids",
            "data_type": "not in list of valid types",
            "file_extension": "not a valid primary file extension",
            "extract_types": "at least one extract type given not in list of valid extract types",
            "factor": "could not be converted to float",
            "day_range": "could not be converted to int",
            "string": "could not be converted to string",
            "group_class": "not a valid group class"
        }

        # current datapackage fields
        self.fields = json.load(open(self.dir_base + "/fields.json", 'r'), object_pairs_hook=OrderedDict)

        # mongo stuff
        self.use_mongo = False

        self.client = None # pymongo.MongoClient()
        self.db = None # self.client.asdf
        self.c_data = None # self.db.data

        self.group_exists = False
        self.actual_exists = {}
        self.is_actual = False

        self.new_boundary = False
        self.update_geometry = False


    # -------------------------
    #  misc functions


    # set file format 
    def update_file_format(self, val):
        self.file_format = val


    # -------------------------
    # input validation functions


    # base path exists and is a directory
    # ***going to add check to make sure path is in REU/data/internal(?) folder***
    def is_dir(self, val):
        return os.path.isdir(str(val)), str(val), self.error["is_dir"]


    # check if name is unique and valid
    def name(self, val):
        val = re.sub(' ', '_', val)
        val = re.sub('[^0-9a-zA-Z._-]+', '', val)

        if len(val) < 5:
            return False, None, "Name must be at least 5 (valid) chars"


        val = val.lower()
        
        if self.interface and not p.user_prompt_use_input(value=val):
            return False, None, "User rejected input"
        
        if not self.use_mongo:
            self.use_mongo = True
            self.client = pymongo.MongoClient()
            self.db = self.client.asdf
            self.c_data = self.db.data

        # check mongodb
        if not "name" in self.data or ("name" in self.data and val != self.data["name"]):
            unique_search = self.c_data.find({"name": val}).limit(1)

            unique = unique_search.count() == 0

            if not unique and unique_search[0]["base"] != self.data["base"]:
                return False, None, "Name matches an existing dataset (names must be unique)"
        

        return True, val, None


    def mini_name(self, val):

        val = re.sub(' ', '_', val)
        val = re.sub('[^0-9a-zA-Z._-]+', '', val)

        if len(val) != 4:
            return False, None, "Mini name must be at 4 (valid) chars"

        val = val.lower()
        
        if self.interface and not p.user_prompt_use_input(value=val):
            return False, None, "User rejected input"
        
        if not self.use_mongo:
            self.use_mongo = True
            self.client = pymongo.MongoClient()
            self.db = self.client.asdf
            self.c_data = self.db.data

        # check mongodb
        if not "mini_name" in self.data or ("mini_name" in self.data and val != self.data["mini_name"]):
            unique_search = self.c_data.find({"mini_name": val}).limit(1)

            unique = unique_search.count() == 0

            if not unique and unique_search[0]["base"] != self.data["base"]:
                return False, None, "Mini name matches an existing dataset (names must be unique)"
        

        return True, val, None


    # each extract type in extract_types
    def license_types(self, val):
        valx = [x.strip(' ') for x in val.split(",")]

        if len(valx) == 1 and valx[0] == "":
            return True, [], None

        valid = False not in [x in self.types["licenses"] for x in valx]

        vals = None
        if valid:
            vals = [self.licenses[x] for x in valx]

        return valid, vals, self.error["license_types"]


    # type in types
    def data_type(self, val):
        return val in self.types["data"], val, self.error["data_type"]


    # each extract type in extract_types
    def file_extension(self, val):
        valid = self.file_format in self.types["file_extensions"].keys() and val in self.types["file_extensions"][self.file_format]
        return valid, val, self.error["file_extension"]

    # each extract type in extract_types
    def extract_types(self, val):
        vals = [x.strip(' ') for x in val.split(",")]
        valid = False not in [x in self.types["extracts"] for x in vals]
        return valid, vals, self.error["extract_types"]


    # factor is a float
    def factor(self, val):
        if val == "":
            val = 1.0

        try:
            float(val)
            return True, float(val), None
        except:
            return False, None, self.error["factor"]


    # day_range is string
    def day_range(self, val):
        if val == "":
            val = 1

        try:
            int(float(val))
            return True, int(float(val)), None
        except:
            return False, None, self.error["day_range"]


    # generic string
    def string(self, val):
        try:
            str(val)
            return True, str(val), None
        except:
            return False, None, self.error["string"]


    # boundary group
    def group(self, val):

        # core database is already named data
        # cannot have tracker database with same name
        if val == "data":
            return False, None, "group name can not be \"data\""


        if not self.use_mongo:
            self.use_mongo = True
            self.client = pymongo.MongoClient()
            self.db = self.client.asdf
            self.c_data = self.db.data


        val = str(val)

        name = self.data["name"]

        # group is boundary name if empty
        if val == "":
            val = name

        # check if boundary with group exists
        exists = self.c_data.find({"type": "boundary", "options.group": val}).limit(1).count() > 0
       
        # if script is in auto mode then it assumes you want to continue with group name and with existing actual
        if not exists and self.interface and not p.user_prompt_bool("Group \""+val+"\" does NOT exist. Are you sure you want to create it?" ):
            return False, None, "group did not pass due to user request - new group"
        
        elif exists:
            actual_exists = self.c_data.find({"type": "boundary", "options.group": val, "options.group_class": "actual"}).limit(1).count() > 0

            if not actual_exists and self.interface and not p.user_prompt_bool("The actual boundary for group \""+val+"\" exists. Do you wish to continue?" ):
                return False, None, "group did not pass due to user request - existing group actual"

        return True, str(val), None


    # boundary group_class
    def group_class(self, val):
        return val in self.types["group_class"], val, self.error["group_class"]


    # runs check on selected group to determine parameters used in group_check selection
    # parameters: group_exists, actual_exists, is_actual
    def run_group_check(self, group):
        # check if boundary with group exists
        exists = self.c_data.find({"type": "boundary", "options.group": group}).limit(1).count() > 0
       
        self.actual_exists = {}
        self.actual_exists[val] = False

        if exists:
            self.group_exists = True

            search_actual = self.c_data.find({"type": "boundary", "options.group": val, "options.group_class": "actual"}).limit(1)
            self.actual_exists[val] =  search_actual.count() > 0

            if self.actual_exists[val]:

                # case where updating actual
                if search_actual[0]["base"] == self.data["base"]:
                    self.is_actual = True
            
