# add dataset to system
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)

# -------------------------------------

# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))

# -----------------------------------------------------------------------------

from pprint import pprint
import datetime
import json
import pymongo
import fiona

from resource_utility import ResourceTools


# -----------------------------------------------------------------------------

parent = os.path.dirname(os.path.abspath(__file__))
script = os.path.basename(sys.argv[0])
version = "0.4"
generator = "auto"

# -----------------------------------------------------------------------------


def quit(reason):

    # do error log stuff
    #

    # output error logs somewhere
    #

    # if auto, move job file to error location
    #

    sys.exit("add_gadm.py: Terminating script - "+str(reason)+"\n")


# init data package
doc = {}

# get release base path
if len(sys.argv) > 2:

    path = sys.argv[2]

    if os.path.isdir(path):
        doc['base'] = path
    else:
        quit("Invalid base directory provided.")

else:
    quit("No base directory provided")


# add version input here
if len(sys.argv) > 3:

    gadm_version = sys.argv[3]

    try:
        gadm_version = float(gadm_version)
    except:
        quit("Invalid GADM version provided.")

else:
    quit("No GADM version provided")


# optional arg
# mainly for user to specify manual run
if len(sys.argv) > 4:
    if sys.argv[4] in ['auto', 'manual']:
        generator = sys.argv[4]
    else:
        quit("Invalid additional inputs")


# exit if too many args
if len(sys.argv) > 5:
        quit("Invalid inputs arguments count")


# remove trailing slash from path
if doc["base"].endswith("/"):
    doc["base"] = doc["base"][:-1]


doc["asdf"] = {}
doc["asdf"]["date_added"] = str(datetime.date.today())
doc["asdf"]["date_updated"] = str(datetime.date.today())
doc["asdf"]["script"] = script
doc["asdf"]["version"] = version
doc["asdf"]["generator"] = generator

doc["type"] = "boundary"
doc["file_format"] = "vector"
doc["file_extension"] = "geojson"
doc["file_mask"] = "None"

# -------------------------------------


gadm_name = os.path.basename(doc["base"])

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

doc["extras"]["citation"] = "Global Administrative Areas (GADM) http://www.gadm.org."
doc["extras"]["sources_web"] = "http://www.gadm.org"
doc["extras"]["sources_name"] = "Global Administrative Areas (GADM)"

doc["extras"]["gadm_country"] = gadm_country
doc["extras"]["gadm_iso3"] = gadm_iso3
doc["extras"]["gadm_adm"] = int(gadm_adm[-1:])
doc["extras"]["gadm_name"] = "PLACEHOLDER"

try:
    doc["options"]["group_title"] = "{0} GADM {1}".format(gadm_country,
                                                         gadm_version)
except Exception as e:
    print gadm_country
    print gadm_version
    raise Exception(e)


# v = ValidationTools()

# probably do not need this
# run check on group to prep for group_class selection
# v.run_group_check(doc['options']['group'])


# boundary group
if "adm0" in gadm_name.lower():
     doc["options"]["group_class"] = "actual"
else:
     doc["options"]["group_class"] = "sub"


doc["active"] = 0


# -----------------------------------------------------------------------------

# resource utils class instance
ru = ResourceTools()

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
ru.temporal["name"] = "Temporally Invariant"
ru.temporal["format"] = "None"
ru.temporal["type"] = "None"
ru.temporal["start"] = 10000101
ru.temporal["end"] = 99991231

doc["temporal"] = ru.temporal


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
    doc["extras"]["gadm_name"] = tmp_feature['properties']['ENGTYPE_'+ gadm_adm[-1:]]

doc["description"] = "GADM Boundary File for {0} ({1}) in {2}.".format(
    gadm_adm.upper(), doc["extras"]["gadm_name"], gadm_country)


# reorder resource fields
# resource_order = ["name", "path", "bytes", "start", "end"]
# resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

# update main list
ru.resources.append(resource_tmp)

doc["resources"] = ru.resources


# -----------------------------------------------------------------------------
# database update(s) and datapackage output

print "\nFinal document..."
pprint(doc)


# update mongo
print "\nWriting datapackage to system..."

# connect to database and asdf collection
client = pymongo.MongoClient(config.server)
db_asdf = client.asdf
db_tracker = client.trackers


gadm_col_str = "data"
# gadm_col_str = "gadm" + str(gadm_version).replace('.', '')

# prep collection if needed
if not gadm_col_str in db_asdf.collection_names():
    c_data = db_asdf[gadm_col_str]

    c_data.create_index("base", unique=True)
    c_data.create_index("name", unique=True)
    c_data.create_index([("spatial", pymongo.GEOSPHERE)])

else:
    c_data = db_asdf[gadm_col_str]


# update core
# try:
c_data.replace_one({"base": doc["base"]}, doc, upsert=True)
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

    # # add each non-boundary dataset item to new boundary collection with "unprocessed" flag
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


print "\nadd_gadm.py: Done.\n"

