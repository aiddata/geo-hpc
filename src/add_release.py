# add dataset to system
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database

# -----------------------------------------------------------------------------

import sys
import os
import datetime
import json
import pymongo

import release_to_mongo
from resource_utility import ResourceTools


def run(path=None, generator="auto", client=None, config=None):


    script = os.path.basename(__file__)
    version = "0.4"
    default_generator = "auto"


    # -------------------------------------------------------------------------


    def quit(reason):

        # do error log stuff
        #

        # output error logs somewhere
        #

        # if auto, move job file to error location
        #

        sys.exit("add_release.py: terminating script - " + str(reason) + "\n")


    # init data package
    dp = {}

    # get release base path
    if path is not None:

        if os.path.isdir(path):
            dp['base'] = path
        else:
            quit("Invalid base directory provided.")

    else:
        quit("No base directory provided")


    # optional arg
    # mainly for user to specify manual run
    if generator is None:
        generator = default_generator

    elif generator not in ['auto', 'manual']:
        quit("Invalid additional inputs")


    if client is None:
        quit("No mongodb client connection provided")

    if config is None:
        quit("No config object provided")


    # remove trailing slash from path
    if dp["base"].endswith("/"):
        dp["base"] = dp["base"][:-1]


    dp["asdf_date_added"] = str(datetime.date.today())
    dp["asdf_date_updated"] = str(datetime.date.today())
    dp["asdf_script"] = script
    dp["asdf_version"] = version
    dp["asdf_generator"] = generator

    dp["type"] = "release"
    dp["file_format"] = "release"
    dp["file_extension"] = ""
    dp["file_mask"] = "None"

    # -------------------------------------

    # get release datapackage
    release_path = dp["base"] + '/datapackage.json'
    release_package = json.load(open(release_path, 'r'))

    for f in release_package.keys():

        if f not in ['resources', 'extras']:
            rkey = f.replace (" ", "_").lower()
            dp[f] = release_package[f]

        elif f == 'extras':
            for g in release_package['extras']:
                rkey = g['key'].replace (" ", "_").lower()
                dp[rkey] = g['value']


    dp["active"] = 1


    # -----------------------------------------------------------------------------

    # resource utils class instance
    ru = ResourceTools()

    print "\nProcessing temporal..."

    # set temporal using release datapackage
    ru.temporal["name"] = dp['temporal_name']
    ru.temporal["format"] = "%Y"
    ru.temporal["type"] = "year"
    ru.temporal["start"] = dp['temporal_start']
    ru.temporal["end"] = dp['temporal_end']


    # -------------------------------------
    print "\nProcessing spatial..."

    # get extemt
    loc_table_path = dp['base'] + "/data/locations.csv"
    geo_ext = ru.release_envelope(loc_table_path)

    # clip extents if they are outside global bounding box
    for c in range(len(geo_ext)):

        if geo_ext[c][0] < -180:
            geo_ext[c][0] = -180
        elif geo_ext[c][0] > 180:
            geo_ext[c][0] = 180

        if geo_ext[c][1] < -90:
            geo_ext[c][1] = -90
        elif geo_ext[c][1] > 90:
            geo_ext[c][1] = 90


    # display datset info to user
    print "Dataset bounding box: ", geo_ext

    # check bbox size
    xsize = geo_ext[2][0] - geo_ext[1][0]
    ysize = geo_ext[0][1] - geo_ext[1][1]
    tsize = abs(xsize * ysize)

    scale = "regional"
    if tsize >= 32400:
        scale = "global"

    dp["scale"] = scale

    # spatial
    ru.spatial = {
        "type": "Polygon",
        "coordinates": [ [
            geo_ext[0],
            geo_ext[1],
            geo_ext[2],
            geo_ext[3],
            geo_ext[0]
        ] ]
    }


    # -------------------------------------
    print '\nProcessing resources...'

    resource_tmp = {
        "name": dp['name'],
        "bytes": 0,
        "path": "",
        "start": ru.temporal['start'],
        "end": ru.temporal['end'],
        "reliability": False
    }

    # resource_order = ["name", "path", "bytes", "start", "end", "reliability"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)
    ru.resources.append(resource_tmp)


    # -------------------------------------
    # add temporal, spatial and resources info

    dp["temporal"] = ru.temporal
    dp["spatial"] = ru.spatial
    dp["resources"] = ru.resources


    # -----------------------------------------------------------------------------
    # database update(s) and datapackage output

    print "\nFinal datapackage..."
    print dp

    # json_out = '/home/userz/Desktop/summary.json'
    # json_handle = open(json_out, 'w')
    # json.dump(dp, json_handle, sort_keys=False, indent=4,
    #           ensure_ascii=False)


    # update mongo
    print "\nWriting datapackage to mongo..."


    # connect to database and asdf collection
    client = pymongo.MongoClient(config.server)
    asdf = client[config.asdf_db]


    # prep collection if needed
    if not "data" in asdf.collection_names():
        c_data = asdf["data"]

        c_data.create_index("base", unique=True)
        c_data.create_index("name", unique=True)
        c_data.create_index([("spatial", pymongo.GEOSPHERE)])

    else:
        c_data = asdf["data"]


    # update core
    # try:
    c_data.replace_one({"base": dp["base"]}, dp, upsert=True)
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

    # bnds = self.c_data.find({"type": "boundary", "options.group_class": "actual"}, {"options": 1})
    # for bnd in bnds:
    #     c_bnd = self.asdf[bnd["options"]["group"]]
    #     # c_bnd.insert(dset)
    #     c_bnd.replace_one({"name": dset["name"]}, dset, upsert=True)


    # create mongodb for dataset
    # ru.release_to_mongo(dp['name'], dp['base'], client)

    release_to_gadm.run(name=dp['name'], path=dp['base'],
                        client=client, config=config)

    print "\nDone.\n"



if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   branch (required)
    #   absolute path to release (required)
    #   generator (optional, defaults to "manual")

    # import sys
    # import os

    branch = sys.argv[0]

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

    # -----------------------------------------------------------------------------

    client = pymongo.MongoClient(config.server)

    if len(generator) < 3:
        main_generator = "manual"
    else:
        main_generator = sys.argv[2]

    run(path=sys.argv[1], generator=main_generator, client=client, config=config)
