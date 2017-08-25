"""
add raster dataset to asdf
    - validate options
    - generate metadata for dataset resources
    - create document
    - update mongo database
"""

import sys
import os
import re
from pprint import pprint
import datetime
import json
import pymongo
from warnings import warn

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

import ingest_resources as ru
from ingest_validation import ValidationTools
from ingest_database import MongoUpdate


def run(path=None, client=None, version=None, config=None,
        generator="auto", update=False, dry_run=False):

    print '\n---------------------------------------'

    script = os.path.basename(__file__)

    def quit(reason):
        """quit script cleanly

        to do:
        - do error log stuff
        - output error logs somewhere
        - if auto, move job file to error location
        """
        raise Exception("{0}: terminating script - {1}\n".format(
            script, reason))


    if config is not None:
        client = config.client
    elif client is not None:
        config = client.info.config.findOne()
    else:
        quit('Neither config nor client provided.')

    version = config.versions["asdf-rasters"]

    # update mongo class instance
    dbu = MongoUpdate(client)

    # -------------------------------------


    # check path
    if path is not None:
        if not os.path.exists(path):
            quit("Invalid path provided.")
    else:
        quit("No path provided")


    # optional arg - mainly for user to specify manual run
    if generator not in ['auto', 'manual']:
        quit("Invalid generator input")


    if client is None:
        quit("No mongodb client connection provided")

    if config is None:
        quit("No config object provided")


    raw_update = update
    if update in ["partial", "meta"]:
        update = "partial"
    elif update in ["update", True, 1, "True", "full", "all"]:
        update = "full"
    else:
        update = False

    print "running update status `{0}` (input: `{1}`)".format(
        update, raw_update)

    if dry_run in ["false", "False", "0", "None", "none", "no"]:
        dry_run = False

    dry_run = bool(dry_run)
    if dry_run:
        print "running dry run"


    # init document
    doc = {}

    doc["asdf"] = {}
    doc["asdf"]["script"] = script
    doc["asdf"]["version"] = version
    doc["asdf"]["generator"] = generator
    doc["asdf"]["date_updated"] = str(datetime.date.today())
    if not update:
        doc["asdf"]["date_added"] = str(datetime.date.today())

    # -------------------------------------

    # get inputs
    if os.path.isfile(path):
        data = json.load(open(path, 'r'))
    else:
        quit("invalid input file path")


    required_core_fields = [
        "base", "type", "file_extension", "file_mask",
        "name", "title", "description", "version", "active"
    ]

    missing_core_fields = [i for i in required_core_fields
                           if i not in data]

    if len(missing_core_fields) > 0:
        quit("Missing core fields ({0})".format(missing_core_fields))


    existing_original = None
    if update:
        if not "data" in client.asdf.collection_names():
            update = False
            msg = "Update specified but no data collection exists."
            if generator == "manual":
                raise Exception(msg)
            else:
                warn(msg)
        else:
            base_original = client.asdf.data.find_one({'base': data["base"]})
            if base_original is None:
                update = False
                msg = "Update specified but no existing dataset found."
                if generator == "manual":
                    raise Exception(msg)
                else:
                    warn(msg)

    # -------------------------------------

    # validate class instance
    v = ValidationTools(client)


    # validate base path
    valid_base = v.base(data["base"], update)

    if not valid_base.isvalid:
        quit(valid_base.error)

    doc["base"] = valid_base.value
    base_exists = valid_base.data['exists']


    # validate name
    valid_name = v.name(data["name"], update)

    if not valid_name.isvalid:
        quit(valid_name.error)

    doc["name"] = valid_name.value
    name_exists = valid_name.data['exists']


    if update:
        if not base_exists and not name_exists:
            warn(("Update specified but no dataset with matching "
                  "base ({0}) or name ({1}) was found").format(doc["base"],
                                                               doc["name"]))

        elif base_exists and name_exists:

            base_id = str(valid_base.data['search']['_id'])
            name_id = str(valid_name.data['search']['_id'])

            if base_id != name_id:
                quit("Update option specified but identifying fields (base "
                     "and name) belong to different existing datasets."
                     "\n\tBase: {0}\n\tName: {1}".format(doc["base"],
                                                         doc["name"]))
            else:
                existing_original = valid_name.data['search']

        elif name_exists:
            existing_original = valid_name.data['search']

        elif base_exists:
            existing_original = valid_base.data['search']

        doc["asdf"]["date_added"] = existing_original["asdf"]["date_added"]
        # doc["active"] = existing_original["active"]


    # validate type and set file_format
    valid_type = v.data_type(data["type"])

    if not valid_type.isvalid:
        quit(valid_type.error)

    doc["type"] = valid_type.value
    doc["file_format"] = valid_type.data["file_format"]

    if doc["type"] != "raster":
        quit("Invalid type ({0}), must be raster.".format(doc["type"]))


    # validate file extension (validation depends on file format)
    valid_extension = v.file_extension(data["file_extension"],
                                       doc["file_format"])

    if not valid_extension.isvalid:
        quit(valid_extension.error)

    doc["file_extension"] = valid_extension.value


    # validate title, description and version
    doc["title"] = str(data["title"])
    doc["description"] = str(data["description"])

    doc["details"] = ""
    if "details" in data:
        doc["details"] = str(data["details"])

    doc["version"] = str(data["version"])

    doc["active"] = int(data["active"])


    # validate options for raster

    if not "options" in data:
        quit("Missing options lookup")


    required_options = ["resolution", "extract_types", "factor",
                       "variable_description"]

    missing_options = [i for i in required_options
                       if i not in data["options"]]

    if len(missing_options) > 0:
        quit("Missing fields from options lookup ({0})".format(
            missing_options))


    doc["options"] = {}

    # resolution (in decimal degrees)
    valid_resolution = v.factor(data["options"]["resolution"])

    if not valid_resolution.isvalid:
        quit(valid_resolution.error)

    doc["options"]["resolution"] = valid_resolution.value


    # multiplication factor (if needed, defaults to 1 if blank)
    valid_factor = v.factor(data["options"]["factor"])

    if not valid_factor.isvalid:
        quit(valid_factor.error)

    doc["options"]["factor"] = valid_factor.value

    # ***
    # if factor changes, any extracts adjust with
    # old factor need to be removed
    # ***

    # extract_types (multiple, separate your input with commas)
    valid_extract_types = v.extract_types(data["options"]["extract_types"])

    if not valid_extract_types.isvalid:
        quit(valid_extract_types.error)

    doc["options"]["extract_types"] = valid_extract_types.value


    valid_extract_types_info = v.extract_types(data["options"]["extract_types_info"])

    if not valid_extract_types_info.isvalid:
        quit(valid_extract_types_info.error)

    doc["options"]["extract_types_info"] = valid_extract_types_info.value


    # Description of the variable (units, range, etc.)
    doc["options"]["variable_description"] = str(
        data["options"]["variable_description"])


    # extras
    if not "extras" in data:
        print("Although fields in extras are not required, it may contain "
              "commonly used field which should be added whenever possible "
              "(example: sources_web field)")
        doc["extras"] = {}

    elif not isinstance(data["extras"], dict):
        quit("Invalid instance of extras ({0}) of type: {1}".format(
            data["extras"], type(data["extras"])))
    else:
        doc["extras"] = data["extras"]

    if not "tags" in doc["extras"]:
        doc["extras"]["tags"] = []

    if not "raster" in doc["extras"]["tags"]:
        doc["extras"]["tags"].append("raster")


    if "categorical" in doc["options"]["extract_types"]:
        if not "category_map" in doc["extras"]:
            quit("'categorical' included as extract type but no 'category_map' dict provided in 'extras'.")
        elif not isinstance(doc["extras"]["category_map"], dict):
            quit("The 'category_map' field must be provided as a dict. Invalid type ({0}) given.".format(
                type(doc["extras"]["category_map"])))
        else:
            # make sure category names and values are in proper key:val format
            # and types
            # {"field_name": pixel_value}

            # NOTE: rasterstats requires input cmap as {pixel_value: "field_name"}
            #       this gets switched in extract utility. This was done since using integers
            #       or floats as key values is not valid json and would break ingest jsons
            # (could put int/float as str maybe? then could keep as key)

            # pixel value may be int, float
            # field name may be str, int, float (but only using string for ingest rasters)
            cat_map =  doc["extras"]["category_map"]
            invalid_cat_vals = [i for i in cat_map.values()
                                if not isinstance(i, (int, float))]
            invalid_cat_keys = [i for i in cat_map.keys()
                                if not isinstance(i, basestring)]

            # make sure keys are str
            if invalid_cat_keys:
                print "Invalid `category_map` keys: ({0})".format(invalid_cat_keys)

            # make sure vals or int/float
            if invalid_cat_vals:
                print "Invalid `category_map` values: ({0})".format(invalid_cat_vals)

            if invalid_cat_keys or invalid_cat_vals:
                raise Exception("Invalid `category_map` provided.")

            cat_map = dict(zip(
                [re.sub('[^0-9a-z]', '_', i.lower()) for i in cat_map.keys()],
                cat_map.values()
            ))


    # -------------------------------------

    if update == "partial":
        print "\nProcessed document:"
        pprint(doc)

        print "\nUpdating database (dry run = {0})...".format(dry_run)
        if not dry_run:
            dbu.update(doc, update, existing_original)

        print "\n{0}: Done ({1} update).\n".format(script, update)
        return 0



    # -------------------------------------
    # resource scan

    # find all files with file_extension in path
    file_list = []
    for root, dirs, files in os.walk(doc["base"]):
        for file in files:

            file = os.path.join(root, file)
            file_check = file.endswith('.' + doc["file_extension"])

            if file_check == True:
                file_list.append(file)

        if data["file_mask"] == "None":
            break


    if data["file_mask"] == "None" and len(file_list) != 1:
        quit("Multiple files found when `file_mask = None`")


    # -------------------------------------
    # check file mask

    def validate_file_mask(vmask):
        """Validate a file mask"""

        # designates temporally invariant dataset
        if vmask == "None":
            return True, None

        # test file_mask for first file in file_list
        test_date_str = ru.run_file_mask(vmask, file_list[0], doc["base"])
        valid_date = ru.validate_date(test_date_str)

        if valid_date[0] == False:
            return False, valid_date[1]

        return True, None


    # file mask identifying temporal attributes in path/file names
    valid_file_mask = validate_file_mask(data["file_mask"])

    if valid_file_mask[0]:
        doc["file_mask"] = data["file_mask"]
    else:
        quit(valid_file_mask[1])

    # -------------------------------------
    print "\nProcessing temporal..."

    doc["temporal"] = {}

    if doc["file_mask"] == "None":

        # temporally invariant dataset
        doc["temporal"]["name"] = "Temporally Invariant"
        doc["temporal"]["format"] = "None"
        doc["temporal"]["type"] = "None"
        doc["temporal"]["start"] = 10000101
        doc["temporal"]["end"] = 99991231

    elif len(file_list) > 0:

        # name for temporal data format
        doc["temporal"]["name"] = "Date Range"
        doc["temporal"]["format"] = "%Y%m%d"
        doc["temporal"]["type"] = ru.get_date_range(ru.run_file_mask(
            doc["file_mask"], file_list[0], doc["base"]))[2]
        doc["temporal"]["start"] = None
        doc["temporal"]["end"] = None
        # day range for each file (eg: MODIS 8 day composites)
        # if "day_range" in v.data:
            # "day_range", "File day range? (Must be integer)", v.day_range

    else:
        quit("Warning: file mask given but no resources were found")
        # doc["temporal"]["name"] = "Unknown"
        # doc["temporal"]["format"] = "Unknown"
        # doc["temporal"]["type"] = "Unknown"
        # doc["temporal"]["start"] = "Unknown"
        # doc["temporal"]["end"] = "Unknown"

    # -------------------------------------
    print "\nProcessing spatial..."

    # iterate over files to get bbox and do basic spatial validation
    # (mainly make sure rasters are all same size)
    f_count = 0
    for f in file_list:

        # get basic geo info from each file
        env = ru.raster_envelope(f)
        # get full geo info from first file
        if f_count == 0:
            base_geo = env

            f_count += 1

        # exit if basic geo does not match
        if base_geo != env:
            print f
            print base_geo
            print env
            warn("Raster bounding box does not match")
            # quit("Raster bounding box does not match")


    env = ru.trim_envelope(env)
    print "Dataset bounding box: ", env

    doc["scale"] = ru.envelope_to_scale(env)

    if doc["scale"] == "global":
        print ("This dataset has a bounding box larger than a hemisphere "
               "and will be treated as a global dataset. If this is not a "
               "global (or near global) dataset you may want to turn it into "
               "multiple smaller datasets and ingest them individually.")


    # set spatial
    doc["spatial"] = ru.envelope_to_geom(env)

    # -------------------------------------
    print '\nProcessing resources...'

    resource_list = []

    for f in file_list:
        print f

        # resources
        # individual resource info
        resource_tmp = {}

        # path relative to base
        resource_tmp["path"] = f[f.index(doc["base"]) + len(doc["base"]) + 1:]


        # file size
        resource_tmp["bytes"] = os.path.getsize(f)

        if doc["file_mask"] != "None":
            # temporal
            # get unique time range based on dir path / file names

            # get data from mask
            date_str = ru.run_file_mask(doc["file_mask"], resource_tmp["path"])

            validate_date_str = ru.validate_date(date_str)

            if not validate_date_str[0]:
                quit(validate_date_str[1])


            if "day_range" in doc:
                range_start, range_end, range_type = ru.get_date_range(
                    date_str, doc["day_range"])
            else:
                range_start, range_end, range_type = ru.get_date_range(
                    date_str)

            # name (unique among this dataset's resources,
            # not same name as dataset name)
            resource_tmp["name"] = (doc["name"] +"_"+
                                    date_str["year"] +
                                    date_str["month"] +
                                    date_str["day"])

        else:
            range_start = 10000101
            range_end = 99991231

            resource_tmp["name"] = doc["name"] + "_none"


        # file date range
        resource_tmp["start"] = range_start
        resource_tmp["end"] = range_end

        # # reorder resource fields
        # resource_order = ["name", "path", "bytes", "start", "end"]
        # resource_tmp = OrderedDict((k, resource_tmp[k])
        #                            for k in resource_order)

        # update main list
        resource_list.append(resource_tmp)


        # update dataset temporal info
        if (doc["temporal"]["start"] is None or
                range_start < doc["temporal"]["start"]):
            doc["temporal"]["start"] = range_start
        elif (doc["temporal"]["end"] is None or
                range_end > doc["temporal"]["end"]):
            doc["temporal"]["end"] = range_end


    doc["resources"] = resource_list

    # -------------------------------------
    # database updates

    print "\nProcessed document..."
    pprint(doc)

    print "\nUpdating database (dry run = {0})...".format(dry_run)
    if not dry_run:
        dbu.update(doc, update, existing_original)

    if update:
        print "\n{0}: Done ({1} update).\n".format(script, update)
    else:
        print "\n{0}: Done.\n".format(script)

    print '\n---------------------------------------\n'

    return 0


# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   branch (required)
    #   path (absolute) to release (required)
    #   generator (optional, defaults to "manual")
    #   update (bool)

    branch = sys.argv[1]

    from config_utility import BranchConfig

    config = BranchConfig(branch=branch)

    # check mongodb connection
    if config.connection_status != 0:
        raise Exception("connection status error: {0}".format(
            config.connection_error))


    # -------------------------------------


    path = sys.argv[2]

    generator = sys.argv[3]

    if len(sys.argv) >= 5:
        update = sys.argv[4]
    else:
        update = False

    if len(sys.argv) >= 6:
        dry_run = sys.argv[5]
    else:
        dry_run = False

    run(path=path, config=config, generator=generator,
        update=update, dry_run=dry_run)

