
import os
import json
import re
import pymongo

from collections import OrderedDict



class ValidationResults():
    def __init__(self, original):
        self.original = original

    def error(msg, data=None):
        self.error = msg
        self.data = data
        self.isvalid = False
        self.value = None

    def success(value, data=None):
        self.error = None
        self.data = data
        self.isvalid = True
        self.value = value



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
        c_asdf (): x
        group_exists (): x
        actual_exists (): x
        is_actual (): x

    """
    def __init__(self, client=None):

        # self.interface = False
        # self.user_update = True


        # base path
        # self.dir_base = os.path.dirname(os.path.abspath(__file__))

        # # current datapackage fields
        # self.fields = json.load(open(self.dir_base + "/fields.json", 'r'),
        #                         object_pairs_hook=OrderedDict)


        # acceptable inputs for various fields (dataset types,
        # vector formats, raster formats, etc.)
        self.types = {
            "data": {
                'raster': 'raster',
                'boundary': 'vector',
                # 'polydata': 'vector',
                # 'point': 'vector',
                # 'multipoint': 'vector',
                'release': 'vector'
                # 'document': 'other'
            },
            "file_extensions": {
                "vector": ['geojson', 'shp'],
                "raster": ['tif', 'asc']
            },
            "extracts": ['mean', 'max', 'sum'],
            "group_class": ['actual', 'sub']
        }

        # error messages to go with validation functions
        self.error = {
            "day_range": "could not be converted to int",
            "group_class": "not a valid group class"
        }


        # init mongo
        self.client = client
        self.c_asdf = self.client.asdf.data


        # group / group_class variables
        self.group_exists = False
        self.actual_exists = {}
        self.is_actual = False





# -----------------------------------------------------------------------------
# general validation


    def base(self, val, update=False):
        """Validate and check base path (unique and valid).

        Args:
            base (str): base path used as unique id in mongo
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        # should we check to make sure path is in .../REU/data folder?
        if not os.path.isdir(val):
            msg = "Could not find base directory provided ({0})".format(val)
            out.error(msg)
            return out, None

        # remove trailing slash from path to prevent multiple unique
        # path strings to same dir
        if val.endswith("/"):
            clean = val[:-1]
        else:
            clean = val

        # check mongodb
        search = self.c_asdf.find_one({"base": clean})
        exists = search is not None


        if not update and exists:
            msg = "Dataset with base directory exists ({0})".format(clean)
            out.error(msg, search)
        else:
            out.success(clean, search)

        return out, exists



    # should we check that there are not consection non-alphanumeric chars
    # eg: two underscores, underscore follow by period, etc.
    def name(self, val, update=False):
        """Validate and check name (unique and valid).

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
            ValidationResults instance
        """
        out = ValidationResults(val)

        clean = re.sub(' ', '_', val)
        clean = re.sub('[^0-9a-zA-Z._-]+', '', clean)
        clean = clean.lower()

        if len(clean) < 5:
            msg = "Name must be at least 5 valid chars ({0})".format(clean)
            out.error(msg)
            return out


        # if (self.interface and self.user_update and
        #         not p.user_prompt_use_input(value=clean)):
        #     msg = "User rejected cleaned value ({0})".format(clean)
        #     out.error(msg)


        # check mongodb
        search = self.c_asdf.find_one({"name": clean})
        exists = search is not None

        if not update and exists:
            msg = "Dataset with name exists ({0})".format(clean)
            out.error(msg, search)
        else:
            out.success(clean, search)

        return out, exists



    def data_type(self, val):
        """Validate data type.

        Args:
            val (str): data_type
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        if val not in self.types["data"].keys():
            msg = ("Input type ({0}) not in list of valid data "
                   "types ({1})".format(val, self.types["data"].keys()))
            out.error(msg)
        else:
            data = {'file_format': self.types["data"][val]}
            out.success(val, data)

        return out


    def file_extension(self, val, file_format):
        """Validate file extension.

        Args:
            val (str): file extension
            file_format (str): file format
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        if file_format in self.types["file_extensions"].keys():
            msg = "Input file format ({0}) not in list of valid file format "
                  "({1})".format(file_format,
                                 self.types["file_extensions"].keys())
            out.error(msg)

        elif val not in self.types["file_extensions"][file_format]:
            valid_extensions = self.types["file_extensions"][file_format]
            msg = ("Input file extension ({0}) not in list of valid file "
                   "extensions ({1}) for given file format "
                   "({2})".format(val, valid_extensions, file_format))

            out.error(msg)

        else:
            out.success(val)

        return out


    def string(self, val):
        """Validate value is (or can be converted to) a string.

        Args:
            val (str): string
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)
        try:
            clean = str(val)
            out.success(clean)
        except:
            out.error("Could not convert value to string")

        return out


    def day_range(self, val):
        """Validate day_range is string

        Args:
            val (): day_range
        Returns:
            ValidationResults instance
        """
        if val == "":
            val = 1

        try:
            int(float(val))
            return True, int(float(val)), None
        except:
            return False, None, self.error["day_range"]


# -----------------------------------------------------------------------------
# raster options

    def mini_name(self, val):
        """Validate and check mini_name for dataset types which require it

        Used for any dataset which will be given to used in extract format
        (rasters, points, maybe vectors)

        All characters in mini name must be alphanumeric.
        Spaces and any other characters will be stripped out.
        Resulting mini name must be exactly 4 characters in length.
        All remaining characters will be converted to lower case.

        Final mini name must not exist in data collection.

        Args:
            val (str): mini name
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)


        clean = re.sub(' ', '_', val)
        val = re.sub('[^0-9a-zA-Z]+', '', val)
        clean = clean.lower()

        if len(clean) != 4:
            msg = "Mini name must be at least 4 valid chars ({0})".format(clean)
            out.error(msg)
            return out


        # if (self.interface and self.user_update and
        #         not p.user_prompt_use_input(value=clean)):
        #     msg = "User rejected cleaned value ({0})".format(clean)
        #     out.error(msg)


        # check mongodb
        search = self.c_asdf.find_one({"options.mini_name": clean})
        exists = search is not None

        if not update and exists:
            msg = "Dataset with mini name exists ({0})".format(clean)
            out.error(msg, search)
        else:
            out.success(clean, search)

        return out, exists



    def extract_types(self, val):
        """Validate extract types.

        Args:
            val (): x
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        if isinstance(val, list):
            clean = val
        elif isinstance(val, str):
            clean = [x.strip(' ') for x in val.split(",")]
        else:
            out.error("Invalid type () for extract_types, "
                      "must be list or comma separate string.")
            return out

        if any(i in clean for i in ['all', 'ALL', '*']):
            out.success(self.types["extracts"])
            return out

        invalid = [i for i in clean if i not in self.types["extracts"]]

        if len(invalid) > 0:
            msg = ("Invalid extract type(s) given."
                   "\nInvalid given: {0}"
                   "\nValid extract types: {1}".format(
                    invalid, self.types["extracts"]))
            out.error(msg)
        else:
            out.success(clean)

        return out


    def factor(self, val):
        """Validate factor.

        Must be a float (or be able to be converted to one)

        Args:
            val (): x
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        if val == "":
            val = 1.0

        try:
            clean = float(val)
            out.success(clean)
        except:
            msg = "Factor value could not be converted to float ({0})".format(val)
            out.error(msg)

        return out


# -----------------------------------------------------------------------------
# boundary options

    def group(self, val, update=False):
        """Validate and check boundary group.

        Args:
            val (str): x
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        clean = str(val)

        # check if boundary with group exists
        search = self.c_asdf.find_one({
            "type": "boundary",
            "options.group": clean
        })
        exists = search is not None

        data = {'exists': exists}

        # # if script is in auto mode then it assumes you want to
        # # continue with group name and with existing actual
        # tmp_msg = ("Group \""+clean+"\" does NOT exist. "
        #            "Are you sure you want to create it?")
        # if (not exists and self.interface and
        #         not p.user_prompt_bool(tmp_msg)):
        #     return False, None, ("group did not pass due to "
        #                          "user request - new group")
        # elif exists:

        if exists:
            actual_search = self.c_asdf.find_one({
                "type": "boundary",
                "options.group": clean,
                "options.group_class": "actual"
            })
            actual_exists = actual_search is not None

            data['actual_exists'] = actual_exists

            # tmp_msg = ("The actual boundary for group \""+clean+"\" "
            #            "does not exist. Do you wish to continue?")
            # if (not actual_exists and self.interface and
            #         not p.user_prompt_bool(tmp_msg)):
            #     return False, None, ("group did not pass due to user "
            #                          "request - existing group actual")

        out.success(val, data)
        return out


    def group_class(self, val):
        """Check that boundary group class is valid.

        Args:
            val (str): group class
        Returns:
            ValidationResults instance
        """
        out = ValidationResults(val)

        valid = val in self.types["group_class"]

        if not valid:
            out.error(self.error["group_class"])
        else:
            out.success(val)

        return out


    def run_group_check(self, group):
        """Run check on a boundary group to determine parameters used
        in group_check selection

        Parameters which are set:
            group_exists (bool): if the group exists
            actual_exists (bool): if the actual boundary for the group
                                  exists yet
            is_actual (bool): if this dataset is the boundary used to
                              define the group

        Args:
            group (str): group name
        """

        # check if boundary with group exists
        exists = self.c_asdf.find({
            "type": "boundary",
            "options.group": group
        }).limit(1).count() > 0

        self.actual_exists = {}
        self.actual_exists[group] = False

        if exists:
            self.group_exists = True

            search_actual = self.c_asdf.find({
                "type": "boundary",
                "options.group": group,
                "options.group_class": "actual"
            }).limit(1)
            self.actual_exists[group] =  search_actual.count() > 0

            if self.actual_exists[group]:

                # case where updating actual
                if search_actual[0]["base"] == self.data["base"]:
                    self.is_actual = True

