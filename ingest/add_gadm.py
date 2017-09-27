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
from unidecode import unidecode
import pymongo
from warnings import warn

import fiona

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

import ingest_resources as ru
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

    version = config.versions["asdf-gadm"]

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
    elif update in ["missing"]:
        update = "missing"
    else:
        update = False

    print "running update status `{0}` (input: `{1}`)".format(
        update, raw_update)

    if dry_run in ["false", "False", "0", "None", "none", "no"]:
        dry_run = False

    dry_run = bool(dry_run)
    if dry_run:
        print "running dry run"

    base_original = client.asdf.data.find_one({'base': path})
    name_original = client.asdf.data.find_one({'name': doc["name"]})

    if not update and base_original is not None:
        msg = "No update specified but dataset exists (base: {0})".format(base_original['base'])
        raise Exception(msg)
    elif not update and name_original is not None:
        msg = "No update specified but dataset exists (name: {0})".format(name_original['name'])
        raise Exception(msg)


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
            if base_original is None and update != "missing":
                update = False
                msg = "Update specified but no existing dataset found."
                if generator == "manual":
                    raise Exception(msg)
                else:
                    warn(msg)


    # init document
    doc = {}

    doc["asdf"] = {}
    doc["asdf"]["script"] = script
    doc["asdf"]["version"] = version
    doc["asdf"]["generator"] = generator
    doc["asdf"]["date_updated"] = str(datetime.date.today())
    if not update or update == "missing":
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


    # -------------------------------------

    gadm_name = os.path.basename(doc["base"])

    gadm_version = os.path.basename(os.path.dirname(path))[4:]

    gadm_iso3 = gadm_name[:3]
    gadm_adm = gadm_name[4:]

    # active_iso3_list = config.release_iso3.values() + config.other_iso3
    # is_active = gadm_iso3.upper() in active_iso3_list

    inactive_iso3_list = config.inactive_iso3
    is_active = gadm_iso3.upper() not in inactive_iso3_list


    doc["active"] = int(is_active)

    parent = os.path.dirname(os.path.abspath(__file__))
    gadm_lookup_path = parent + '/gadm_iso3.json'
    gadm_lookup =  json.load(open(gadm_lookup_path, 'r'))

    gadm_country = unidecode(gadm_lookup[gadm_iso3])

    doc["name"] = (gadm_iso3.lower() + "_" + gadm_adm.lower() + "_gadm" +
                   gadm_version.replace('.', ''))

    if update:

        if update == "missing" and name_original is not None and base_original is not None:
            warn("Dataset exists (running in 'missing' update mode). Running partial update and setting to active (if possible).")
            update = "partial"

        if update != "missing":
            if name_original is None and base_original is None:
                update = False
                warn(("Update specified but no dataset with matching "
                      "base ({0}) or name ({1}) was found").format(doc["base"],
                                                                   doc["name"]))

                # in case we ended up not finding a match for name
                doc["asdf"]["date_added"] = str(datetime.date.today())

            elif name_original is not None and base_original is not None:

                if str(name_original['_id']) != str(base_original['_id']):
                    quit("Update option specified but identifying fields (base "
                         "and name) belong to different existing datasets."
                         "\n\tBase: {0}\n\tName: {1}".format(doc["base"],
                                                             doc["name"]))
                else:
                    existing_original = name_original

            elif name_original is not None:
                existing_original = name_original

            elif base_original is not None:
                existing_original = base_original


            doc["asdf"]["date_added"] = existing_original["asdf"]["date_added"]
            # doc["active"] = existing_original["active"]


    doc["title"] = " ".join([gadm_country, gadm_adm.upper(), "Boundary - GADM", gadm_version])

    doc["description"] = "PLACEHOLDER"

    doc["version"] = gadm_version


    doc["options"] = {}
    doc["options"]["group"] = (gadm_iso3.lower() + "_gadm" +
                               gadm_version.replace('.', ''))

    doc["extras"] = {}

    doc["extras"]["citation"] = ("Global Administrative Areas "
                                 "(GADM) http://www.gadm.org.")
    doc["extras"]["sources_web"] = "http://www.gadm.org"
    doc["extras"]["sources_name"] = "Global Administrative Areas (GADM)"

    doc["extras"]["gadm_country"] = gadm_country
    doc["extras"]["gadm_iso3"] = gadm_iso3
    doc["extras"]["gadm_adm"] = int(gadm_adm[-1:])
    doc["extras"]["gadm_unit"] = "PLACEHOLDER"
    doc["extras"]["tags"] = ["gadm", gadm_adm, gadm_country]

    doc["options"]["group_title"] = "{0} GADM {1}".format(gadm_country,
                                                          gadm_version)

    # boundary group
    if "adm0" in gadm_name.lower():
         doc["options"]["group_class"] = "actual"
         doc["active"] = 0
    else:
         doc["options"]["group_class"] = "sub"

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

    # get adm unit name for country and add to gadm info and description
    if gadm_adm.lower() == "adm0":
        gadm_unit = "Country"
    else:
        with fiona.open(f, 'r') as tmp_feature_src:
            tmp_feature = tmp_feature_src[0]
            gadm_unit = tmp_feature['properties']['ENGTYPE_'+ gadm_adm[-1:]]

    doc["extras"]["gadm_unit"] = gadm_unit
    if gadm_unit not in [None, "Unknown"]:
        doc["extras"]["tags"].append(gadm_unit)
    doc["description"] = "GADM Boundary File for {0} ({1}) in {2}.".format(
        gadm_adm.upper(), gadm_unit, gadm_country)

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

    print "\nProcessed document:"
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

