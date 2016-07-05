"""
add gadm dataset to asdf
    - build gadm specific dataset options
    - generate metadata for dataset resources
    - create document
    - update mongo database
"""

import sys
import os

from pprint import pprint
import datetime
import json
import pymongo

import fiona
from resource_utility import ResourceTools
from mongo_utility import MongoUpdate


def run(path=None, client=None, config=None, generator="auto", update=False):

    parent = os.path.dirname(os.path.abspath(__file__))

    script = os.path.basename(__file__)
    version = config.versions["asdf-gadm"]


    # resource utils class instance
    ru = ResourceTools()

    # update mongo class instance
    update_db = MongoUpdate(client)


    # -------------------------------------------------------------------------

    def quit(reason):
        """quit script cleanly

        to do:
        - do error log stuff
        - output error logs somewhere
        - if auto, move job file to error location
        """
        sys.exit("{0}: terminating script - {1}\n").format(script, reason)



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


    if update in ["update", True, 1, "True"]:
        update = True
        if not "data" in client.asdf.collection_names():
            raise Exception("update specified but no data collection exists")

        original = client.asdf.data.find_one({'base': path})
        if original is None:
            raise Exception("update specified but no dataset with matching "
                            "base exists")
    else:
        update = False


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

    if os.path.isdir(path):
        # remove trailing slash from path
        if path.endswith("/"):
            path = path[:-1]
    else:
        quit("Invalid base directory provided.")

    # -------------------------------------

    doc['base'] = path

    doc["type"] = "boundary"
    doc["file_format"] = "vector"
    doc["file_extension"] = "geojson"
    doc["file_mask"] = "None"

    doc["active"] = 0


    gadm_name = os.path.basename(doc["base"])

    gadm_version = os.path.basename(os.path.dirname(os.path.dirname(path)))[4:]

    gadm_iso3 = gadm_name[:3]
    gadm_adm = gadm_name[4:]

    gadm_lookup_path = parent + '/gadm_iso3.json'
    gadm_lookup =  json.load(open(gadm_lookup_path, 'r'))

    gadm_country = gadm_lookup[gadm_iso3].encode('utf8')

    doc["name"] = (gadm_iso3.lower() + "_" + gadm_adm.lower() + "_gadm" +
                 str(gadm_version).replace('.', ''))

    doc["title"] = (gadm_country + " " + gadm_adm.upper() +
                  " Boundary - GADM " + str(gadm_version))

    doc["description"] = "PLACEHOLDER"

    doc["version"] = gadm_version


    doc["options"] = {}
    doc["options"]["group"] = (gadm_iso3.lower() + "_gadm" +
                             str(gadm_version).replace('.', ''))

    doc["extras"] = {}

    doc["extras"]["citation"] = ("Global Administrative Areas "
                                 "(GADM) http://www.gadm.org.")
    doc["extras"]["sources_web"] = "http://www.gadm.org"
    doc["extras"]["sources_name"] = "Global Administrative Areas (GADM)"

    doc["extras"]["gadm_country"] = gadm_country
    doc["extras"]["gadm_iso3"] = gadm_iso3
    doc["extras"]["gadm_adm"] = int(gadm_adm[-1:])
    doc["extras"]["gadm_name"] = "PLACEHOLDER"
    doc["extras"]["tags"] = ["gadm"]

    doc["options"]["group_title"] = "{0} GADM {1}".format(gadm_country,
                                                          gadm_version)


    # boundary group
    if "adm0" in gadm_name.lower():
         doc["options"]["group_class"] = "actual"
    else:
         doc["options"]["group_class"] = "sub"


    # -------------------------------------------------------------------------
    # resource scan

    # find all files with file_extension in path
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


    # get adm unit name for country and add to gadm info and description
    tmp_feature = fiona.open(f, 'r').next()

    if gadm_adm.lower() == "adm0":
        doc["extras"]["gadm_name"] = "Country"
    else:
        doc["extras"]["gadm_name"] = (
            tmp_feature['properties']['ENGTYPE_'+ gadm_adm[-1:]])

    doc["description"] = "GADM Boundary File for {0} ({1}) in {2}.".format(
        gadm_adm.upper(), doc["extras"]["gadm_name"], gadm_country)


    # reorder resource fields
    # resource_order = ["name", "path", "bytes", "start", "end"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

    # update main list
    resource_list = [resource_tmp]

    doc["resources"] = resource_list


    # -------------------------------------------------------------------------
    # database updates

    print "\nFinal document..."
    pprint(doc)


    print "\nUpdating database..."


    update_spatial = False
    if update and doc['spatial'] != original['spatial']:
        update_spatial = True

    # =======================================


    # connect to database and asdf collection
    db_asdf = client.asdf
    db_tracker = client.trackers

    # prep collection if needed
    if not "data" in db_asdf.collection_names():
        c_data = db_asdf.data

        c_data.create_index("base", unique=True)
        c_data.create_index("name", unique=True)
        c_data.create_index([("spatial", pymongo.GEOSPHERE)])

    else:
        c_data = db_asdf.data


    # update core
    # try:
    c_data.replace_one({"base": doc["base"]}, 
                       doc, 
                       upsert=True)
    print "successful core update"
    # except:
    #      quit("Error updating core.")


    # create/initialize tracker
    # try:

    if doc["options"]["group_class"] == "actual":

        # drop boundary tracker if exists
        if doc["options"]["group"] in db_tracker.collection_names():
            db_tracker.drop_collection(doc["options"]["group"])

        # create new boundary tracker collection
        c_bnd = db_tracker[doc["options"]["group"]]
        c_bnd.create_index("name", unique=True)
        # c_bnd.create_index("base", unique=True)
        c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])

        # # add each non-boundary dataset item to new boundary
        # # collection with "unprocessed" flag
        # dsets = c_data.find({"type": {"$ne": "boundary"}})
        # for full_dset in dsets:
        #     dset = {
        #         'name': full_dset["name"],
        #         'spatial': full_dset["spatial"],
        #         'scale': full_dset["scale"],
        #         'status': -1
        #     }
        #     c_bnd.insert(dset)

        print "successful tracker creation"


    # except:
    #      quit("Error updating tracker.")


    # =======================================

    print "\n{0}: Done.\n".format(script)



if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   branch (required)
    #   path (absolute) to release (required)
    #   generator (optional, defaults to "manual")
    #   update (bool)

    # import sys
    # import os

    branch = sys.argv[1]

    branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

    if not os.path.isdir(branch_dir):
        raise Exception('Branch directory does not exist')


    config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
    sys.path.insert(0, config_dir)

    from config_utility import BranchConfig

    config = BranchConfig(branch=branch)

    # -------------------------------------

    # check mongodb connection
    if config.connection_status != 0:
        sys.exit("connection status error: " + str(config.connection_error))

    # -------------------------------------------------------------------------

    client = pymongo.MongoClient(config.server)

    path = sys.argv[2]

    generator = sys.argv[3]

    if len(sys.argv) == 5:
        update = sys.argv[4]
    else:
        update = False


    run(path=path, client=client, config=config,
        generator=main_generator, update=update)

