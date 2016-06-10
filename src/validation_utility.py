
import os
import json
import re
import pymongo

from collections import OrderedDict
from prompt_utility import PromptKit


p = PromptKit()


# validation functions, fields, etc.
class ValidationTools():
    """Validation functions and related variables.

    Attributes:
        interface (): x
        user_update (): x
        data (): x
        dir_base (): x
        licenses (): x
        file_format (): x
        types (): x
        error (): x
        fields (): x
        client (): x
        db (): x
        c_data (): x
        group_exists (): x
        actual_exists (): x
        is_actual (): x
        new_boundary (): x
        update_geometry (): x

    """
    def __init__(self):

        self.interface = False
        self.user_update = True

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
            "data": ['raster', 'boundary', 'release', 'polydata', 'document', 'point', 'multipoint'],
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

        # init mongo
        self.client = pymongo.MongoClient()
        self.c_data = self.client.asdf.data

        # group / group_class variables
        self.group_exists = False
        self.actual_exists = {}
        self.is_actual = False

        # boundary change variables
        self.new_boundary = False
        self.update_geometry = False


    # -------------------------
    #  misc functions


    def datapackage_exists(self, base):
        """Check if datapackage exists in mongo for given base path

        Args:
            base (str): base path used as unique id in mongo
        Returns:
            Tuple containing
                - bool whether datapackage exists
                - Dict if datapackage exists, 0 otherwise
        """
        search = self.c_data.find({"base": base}).limit(1)

        exists = search.count() > 0

        print exists

        datapackage = 0
        if exists:
            datapackage = OrderedDict(search[0])


        return exists, datapackage


    # # check if datapackage json exists for given base path
    # # return datapackage if it does
    # def datapackage_exists(self, base):

    #     exists = os.path.isfile(base+"/datapackage.json")

    #     datapackage = 0
    #     if exists:
    #         datapackage = json.load(open(base+"/datapackage.json", 'r'), object_pairs_hook=OrderedDict)

    #     return exists, datapackage


    # set file format
    def update_file_format(self, val):
        """Set file format."

        Args:
            val (str): file format
        """
        self.file_format = val


    # -------------------------
    # input validation functions


    # ***going to add check to make sure path is in REU/data/internal(?) folder***
    def is_dir(self, val):
        """Check if arg is a directory.

        Args:
            val (str): path
        Returns:
            Tuple containing
                - if path is a directory
                - path
                - associated error message
        """
        return os.path.isdir(str(val)), str(val), self.error["is_dir"]


    # should we check that there are not consection non-alphanumeric chars
    # eg: two underscores, underscore follow by period, etc.
    def name(self, val):
        """Check if name is unique and valid.

        All characters in name must be alphanumeric with the exception of
        underscores, periods or dashes.
        Spaces will be trimmed to single space and replaced with underscore.
        Any other characters will be stripped out.
        Resulting name must be at least 5 characters in length.
        All remaining characters will be converted to lower case.

        Final name must not exist in data collection.

        Args:
            val (str): name
        Returns:
            Tuple containing:
                - if name is valid
                - name
                - None if valid, associated error message if invalid
        """
        val = re.sub(' ', '_', val)
        val = re.sub('[^0-9a-zA-Z._-]+', '', val)

        if len(val) < 5:
            return False, None, "Name must be at least 5 (valid) chars"


        val = val.lower()

        if self.interface and self.user_update and not p.user_prompt_use_input(value=val):
            return False, None, "User rejected input"


        # check mongodb
        if not "name" in self.data or ("name" in self.data and val != self.data["name"]):
            unique_search = self.c_data.find({"name": val}).limit(1)

            unique = unique_search.count() == 0

            if not unique and unique_search[0]["base"] != self.data["base"]:
                return False, None, "Name matches an existing dataset (names must be unique)"


        return True, val, None


    def mini_name(self, val):
        """Validate mini_name for dataset types which require it

        Used for any dataset which will be given to used in extract format (rasters, points, maybe vectors)

        All characters in mini name must be alphanumeric.
        Spaces and any other characters will be stripped out.
        Resulting mini name must be exactly 4 characters in length.
        All remaining characters will be converted to lower case.

        Final mini name must not exist in data collection.

        Args:
            val (str): mini name
        Returns:
            Tuple containing:
                - if mini name is valid
                - mini name
                - None if valid, associated error message if invalid
        """
        val = re.sub(' ', '', val)
        val = re.sub('[^0-9a-zA-Z]+', '', val)

        if len(val) != 4:
            return False, None, "Mini name must be at least 4 (valid) chars"

        val = val.lower()

        if self.interface and self.user_update and not p.user_prompt_use_input(value=val):
            return False, None, "User rejected input"


        # check mongodb
        if not 'options' in self.data or not "mini_name" in self.data['options'] or ("mini_name" in self.data['options'] and val != self.data['options']["mini_name"]):
            unique_search = self.c_data.find({"options.mini_name": val}).limit(1)

            unique = unique_search.count() == 0

            if not unique and unique_search[0]["base"] != self.data["base"]:
                return False, None, "Mini name matches an existing dataset (names must be unique)"


        return True, val, None


    def license_types(self, val):
        """Validate license types.

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        if isinstance(val, list):
            valx = [v['id'] for v in val]
        elif isinstance(val, str):
            valx = [x.strip(' ') for x in val.split(",")]
        else:
            return False, 0, "Invalid input type"


        if len(valx) == 1 and valx[0] == "":
            return True, [], None

        valid = False not in [x in self.types["licenses"] for x in valx]

        vals = None
        if valid:
            vals = [self.licenses[x] for x in valx]

        return valid, vals, self.error["license_types"]


    def data_type(self, val):
        """Validate data type.

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        return val in self.types["data"], val, self.error["data_type"]


    def file_extension(self, val):
        """Validate file extension.

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        valid = self.file_format in self.types["file_extensions"].keys() and val in self.types["file_extensions"][self.file_format]
        return valid, val, self.error["file_extension"]


    def extract_types(self, val):
        """Validate extract types.

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        if isinstance(val, list):
            vals = val
        elif isinstance(val, str):
            vals = [x.strip(' ') for x in val.split(",")]
        else:
            return False, 0, "Invalid input type"

        valid = False not in [x in self.types["extracts"] for x in vals]
        return valid, vals, self.error["extract_types"]


    def factor(self, val):
        """Validate factor.

        Must be a float (or be able to be converted to one)

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        if val == "":
            val = 1.0

        try:
            float(val)
            return True, float(val), None
        except:
            return False, None, self.error["factor"]


    # day_range is string
    def day_range(self, val):
        """

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        if val == "":
            val = 1

        try:
            int(float(val))
            return True, int(float(val)), None
        except:
            return False, None, self.error["day_range"]


    # generic string
    def string(self, val):
        """

        Args:
            val (): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        try:
            str(val)
            return True, str(val), None
        except:
            return False, None, self.error["string"]


    # boundary group
    def group(self, val):
        """Validate boundary group.

        Args:
            val (str): x
        Returns:
            Tuple containing
                -
                -
                -
        """
        # core database is already named data
        # cannot have tracker database with same name
        if val == "data":
            return False, None, "group name can not be \"data\""

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

            if not actual_exists and self.interface and not p.user_prompt_bool("The actual boundary for group \""+val+"\" does not exist. Do you wish to continue?" ):
                return False, None, "group did not pass due to user request - existing group actual"

        return True, str(val), None


    # boundary group_class
    def group_class(self, val):
        """Check that group class is valid.

        Args:
            val (str): group class
        Returns:
            Tuple containing
                - if group class is valid
                - group class
                - associate error message
        """
        return val in self.types["group_class"], val, self.error["group_class"]



    def run_group_check(self, group):
        """Run check on a boundary group to determine parameters used in group_check selection

        Parameters which are set:
            group_exists (bool): if the group exists
            actual_exists (bool): if the actual boundary for the group exists yet
            is_actual (bool): if this dataset is the boundary used to define the group

        Args:
            group (str): group name
        """

        # check if boundary with group exists
        exists = self.c_data.find({"type": "boundary", "options.group": group}).limit(1).count() > 0

        self.actual_exists = {}
        self.actual_exists[group] = False

        if exists:
            self.group_exists = True

            search_actual = self.c_data.find({"type": "boundary", "options.group": group, "options.group_class": "actual"}).limit(1)
            self.actual_exists[group] =  search_actual.count() > 0

            if self.actual_exists[group]:

                # case where updating actual
                if search_actual[0]["base"] == self.data["base"]:
                    self.is_actual = True

