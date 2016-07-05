"""
add release dataset to asdf
    - compile options from release datapackage
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

from resource_utility import ResourceTools
from mongo_utility import MongoUpdate


def run(path=None, client=None, config=None, generator="auto", update=False):

    parent = os.path.dirname(os.path.abspath(__file__))

    script = os.path.basename(__file__)
    version = config.versions["asdf-releases"]

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

    # -------------------------------------

    if os.path.isdir(path):
        # remove trailing slash from path
        if path.endswith("/"):
            path = path[:-1]
    else:
        quit("Invalid base directory provided.")

    # -------------------------------------

    # init document
    doc = {}

    doc["asdf"] = {}
    doc["asdf"]["script"] = script
    doc["asdf"]["version"] = version
    doc["asdf"]["generator"] = generator
    doc["asdf"]["date_updated"] = str(datetime.date.today())
    if not update:
        doc["asdf"]["date_added"] = str(datetime.date.today())

    doc['base'] = path

    doc["type"] = "release"
    doc["file_format"] = "release"
    doc["file_extension"] = ""
    doc["file_mask"] = "None"

    doc["active"] = 1


    # -------------------------------------

    # resource utils class instance
    ru = ResourceTools()

    # get release datapackage
    release_path = doc["base"] + '/datapackage.json'
    release_package = json.load(open(release_path, 'r'))

    core_fields = ['name', 'title', 'description', 'version']

    for f in release_package.keys():

        if f in core_fields:
            rkey = f.replace (" ", "_").lower()
            doc[f] = release_package[f]

        elif f == 'extras':
            doc["extras"] = {}

            for g in release_package['extras']:
                rkey = g['key'].replace (" ", "_").lower()
                doc['extras'][rkey] = g['value']


    # -------------------------------------------------------------------------
    print "\nProcessing temporal..."

    # set temporal using release datapackage
    doc["temporal"] = {}
    doc["temporal"]["name"] = doc['extras']['temporal_name']
    doc["temporal"]["format"] = "%Y"
    doc["temporal"]["type"] = "year"
    doc["temporal"]["start"] = doc['extras']['temporal_start']
    doc["temporal"]["end"] = doc['extras']['temporal_end']


    # -------------------------------------
    print "\nProcessing spatial..."

    # get extemt
    loc_table_path = doc['base'] + "/data/locations.csv"

    env = ru.release_envelope(loc_table_path)
    env = ru.trim_envelope(env)

    print "Dataset bounding box: ", env

    doc["scale"] = ru.envelope_to_scale(env)


    # set spatial
    doc["spatial"] = ru.envelope_to_geom(env)


    # -------------------------------------
    print '\nProcessing resources...'

    resource_tmp = {
        "name": doc['name'],
        "bytes": 0,
        "path": "",
        "start": doc["temporal"]['start'],
        "end": doc["temporal"]['end']
    }

    # resource_order = ["name", "path", "bytes", "start", "end"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)
    resource_list = [resource_tmp]

    doc["resources"] = resource_list


    # -------------------------------------------------------------------------
    # database updates

    print "\nFinal document..."
    pprint(doc)


    print "\nWriting document to mongo..."

    # update mongo class instance
    update_db = MongoUpdate(client)

    update_spatial = False
    if update and doc['spatial'] != original['spatial']:
        update_spatial = True

    # create mongodb for dataset
    update_db.release_to_mongo_wrapper(name=doc['name'], path=doc['base'])

    # =======================================


    db_asdf = client.asdf

    # prep collection if needed
    if not "data" in db_asdf.collection_names():
        c_asdf = db_asdf["data"]

        c_asdf.create_index("base", unique=True)
        c_asdf.create_index("name", unique=True)
        c_asdf.create_index([("spatial", pymongo.GEOSPHERE)])

    else:
        c_asdf = db_asdf["data"]


    # update core
    # try:
    c_asdf.replace_one({"base": doc["base"]}, doc, upsert=True)
    print "successful core update"
    # except:
    #      quit("Error updating core.")


    # # update trackers
    # # add dataset to each boundary collection with "unprocessed" flag
    # dset = {
    #     'name': in_data["name"],
    #     'spatial': in_data["spatial"],
    #     'scale': in_data["scale"],
    #     'status': -1
    # }

    # bnds = self.c_asdf.find({
    #     "type": "boundary",
    #     "options.group_class": "actual"
    #     }, {"options": 1})
    # for bnd in bnds:
    #     c_bnd = self.asdf[bnd["options"]["group"]]
    #     # c_bnd.insert(dset)
    #     c_bnd.replace_one({"name": dset["name"]}, dset, upsert=True)

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


