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

    parent = os.path.dirname(os.path.abspath(__file__))
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

    if doc["type"] != "boundary":
        quit("Invalid type ({0}), must be boundary.".format(doc["type"]))


    # validate file extension (validation depends on file format)
    valid_extension = v.file_extension(data["file_extension"],
                                       doc["file_format"])

    if not valid_extension.isvalid:
        quit(valid_extension.error)

    doc["file_extension"] = valid_extension.value


    # validate title, description and version
    doc["title"] = str(data["title"])
    doc["description"] = str(data["description"])
    doc["version"] = str(data["version"])

    doc["active"] = int(data["active"])


    # validate options for raster

    if not "options" in data:
        quit("Missing options lookup")


    required_options = ["group", "group_title", "group_class"]

    missing_options = [i for i in required_options
                       if i not in data["options"]]

    if len(missing_options) > 0:
        quit("Missing fields from options lookup ({0})".format(
            missing_options))


    doc["options"] = {}


    ###

    warn("Current group checks for boundary do not cover all potential cases "
         "(e.g., geometry changes to group actual, various conflicts based "
         "group_class, existing groups, etc.).")

    # validate group info
    valid_group = v.group(data["options"]["group"], data["options"]["group_class"])

    if not valid_group.isvalid:
        quit(valid_group.error)

    doc["options"]["group"] = valid_group.value

    doc["options"]["group_class"] = data["options"]["group_class"]
    doc["options"]["group_title"] = data["options"]["group_title"]


    ###


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

    if not "boundary" in doc["extras"]["tags"]:
        doc["extras"]["tags"].append("boundary")


    # -------------------------------------
    # resource scan

    # find all files with file_extension in path
    file_list = []
    for root, dirs, files in os.walk(doc["base"]):
        for fname in files:

            fname = os.path.join(root, fname)
            file_check = fname.endswith('.' + doc["file_extension"])

            if file_check == True and not fname.endswith('simplified.geojson'):
                file_list.append(fname)


    if len(file_list) == 0:
        quit("No vector file found in " + doc["base"])

    elif len(file_list) > 1:
        quit("Boundaries must be submitted individually.")


    f = file_list[0]
    print f

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
    print "\nProcessing temporal..."

    # temporally invariant dataset
    doc["temporal"] = {}
    doc["temporal"]["name"] = "Temporally Invariant"
    doc["temporal"]["format"] = "None"
    doc["temporal"]["type"] = "None"
    doc["temporal"]["start"] = 10000101
    doc["temporal"]["end"] = 99991231

    # -------------------------------------
    print "\nProcessing spatial..."

    if not dry_run:
        convert_status = ru.add_asdf_id(f)
        if convert_status == 1:
             quit("Error adding ad_id to boundary file & outputting geojson.")


    env = ru.vector_envelope(f)
    env = ru.trim_envelope(env)
    print "Dataset bounding box: ", env

    doc["scale"] = ru.envelope_to_scale(env)

    # set spatial
    doc["spatial"] = ru.envelope_to_geom(env)

    # -------------------------------------
    print '\nProcessing resources...'

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to base
    resource_tmp["path"] = f[f.index(doc["base"]) + len(doc["base"]) + 1:]

    resource_tmp["name"] = doc["name"]
    resource_tmp["bytes"] = os.path.getsize(f)
    resource_tmp["start"] = 10000101
    resource_tmp["end"] = 99991231

    # reorder resource fields
    # resource_order = ["name", "path", "bytes", "start", "end"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

    # update main list
    resource_list = [resource_tmp]

    doc["resources"] = resource_list

    # -------------------------------------
    # database updates

    print "\nProcessed document..."
    pprint(doc)

    print "\nUpdating database (dry run = {0})...".format(dry_run)
    if not dry_run:
        dbu.update(doc, update, existing_original)
        try:
            dbu.features_to_mongo(doc['name'])
        except:
            # could remove data entry if it cannot be added
            # to mongo. or, at least make sure the data entry is
            # set to inactive
            raise

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

    # import sys
    # import os

    branch = sys.argv[1]

    # branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

    # if not os.path.isdir(branch_dir):
    #     raise Exception('Branch directory does not exist')


    # config_dir = os.path.join(branch_dir, 'asdf', 'src', 'utils')
    # sys.path.insert(0, config_dir)

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

