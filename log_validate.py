import os
import json
from collections import OrderedDict
from log_prompt import prompts

# validation functions, fields, etc.
class validate():

    def __init__(self):

        # store prompt inputs
        self.data = {}

        # base path
        self.dir_base = os.path.dirname(os.path.abspath(__file__))

        # available licenses
        self.licenses = json.load(open(self.dir_base +"/licenses.json"))

        # init file format
        self.file_format = ""

        # acceptable inputs for various fields (dataset types, vector formats, raster formats, etc.)
        self.types = {
            "licenses": self.licenses.keys(),
            "data": ['raster','polydata','document','point','multipoint','boundary'],
            "file_extensions": {
                "vector": ['geojson', 'shp'],
                "raster": ['tif', 'asc']
            },
            "extracts": ['mean', 'max']
        }

        # error messages to go with validation functions
        self.error = {
            "is_dir": "not a directory",
            "name": "name exists or was rejected by user",
            "license_types": "at least one license id given not in list of valid license ids",
            "data_type": "not in list of valid types",
            "file_extension": "not a valid primary file extension",
            "extract_types": "at least one extract type given not in list of valid extract types",
            "factor": "could not be converted to float"
        }

        # current datapackage fields
        self.fields = json.load(open(self.dir_base +"/fields.json"), object_pairs_hook=OrderedDict)


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
        return os.path.isdir(str(val)), str(val)


    # check if name is unique and valid
    def name(self, val):
        val = re.sub('[^0-9a-zA-Z._-]+', '', val)

        if not user_prompt_use_input(value=val):
          return False
        
        # check mongodb
        # 
        # unique = 

        unique = True 
        
        if not unique:   
          return False
        
        return True, val


    # each extract type in extract_types
    def license_types(self, val):
        vals = [x.strip(' ') for x in val.split(",")]
        valid = False not in [x in self.types["licenses"] for x in vals]
        return valid, vals


    # type in types
    def data_type(self, val):
        return val in self.types["data"], val


    # each extract type in extract_types
    def file_extension(self, val):
        valid = self.file_format in self.types["file_extensions"].keys() and val in self.types["file_extensions"][self.file_format]
        return valid, val

    # each extract type in extract_types
    def extract_types(self, val):
        vals = [x.strip(' ') for x in val.split(",")]
        valid = False not in [x in self.types["extracts"] for x in vals]
        return valid, vals


    # factor is a float
    def factor(self, val):
        try:
            float(val)
            return True, float(val)
        except:
            return False
