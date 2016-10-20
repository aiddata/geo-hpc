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
from warnings import warn
import re

util_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'utils')
sys.path.insert(0, util_dir)

import resource_utility as ru
from database_utility import MongoUpdate


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

    version = config.versions["asdf-releases"]

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
            base_original = client.asdf.data.find_one({'base': path})
            if base_original is None:
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

    doc["type"] = "release"
    doc["file_format"] = "release"
    doc["file_extension"] = ""
    doc["file_mask"] = "None"

    # -------------------------------------

    # get release datapackage
    release_path = doc["base"] + '/datapackage.json'
    release_package = json.load(open(release_path, 'r'))

    core_fields = ['name', 'title', 'description', 'version']

    doc["extras"] = {}

    for f in release_package.keys():

        if f in core_fields:
            rkey = f.replace (" ", "_").lower()
            doc[f] = release_package[f]

        elif f == 'extras':

            for g in release_package['extras']:
                rkey = g['key'].replace (" ", "_").lower()
                doc['extras'][rkey] = g['value']


    # updating these fields because
    # - current name is broken (not proper version)
    # - current title and description are not well suited for
    #   general consumption via DET
    doc["extras"]["original_name"] = doc["name"]
    doc["extras"]["original_title"] = doc["title"]
    doc["extras"]["original_description"] = doc["description"]

    doc["name"] = "{0}_{1}_{2}_v{3}".format(
        doc["extras"]["data_set_preamble"].lower(),
        doc["extras"]["data_type"].lower(),
        doc["extras"]["processing_level"].lower(),
        str(doc["version"]).replace(".", "_"))


    preamble_word_list = re.findall(
        '[A-Z](?:[A-Z]*(?![a-z])|[a-z]*)',
        doc["extras"]["data_set_preamble"])

    clean_preamble_word_list = [i for i in preamble_word_list
                                if i not in ["AIMS"]]

    clean_preamble = ' '.join(clean_preamble_word_list)

    doc["title"] = "{0} Geocoded Aid Data v{1}".format(
        clean_preamble, doc["version"])

    doc["description"] = (
        "Aid data from {0} {1}, geocoded and published by AidData. "
        "Covers projects from {1} to {2}. Version {3}.").format(
            clean_preamble,
            doc["extras"]["source_type"],
            doc["extras"]["temporal_start"],
            doc["extras"]["temporal_end"],
            doc["version"])


    doc["extras"]["tags"] = ["aiddata", "geocoded", "release", "socioeconomic"]

    is_active = doc["extras"]["data_set_preamble"] in config.release_iso3
    doc["active"] = int(is_active)

    if update:
        name_original = client.asdf.data.find_one({'name': doc["name"]})

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

    return 0


# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   branch (required)
    #   path (absolute) to release (required)
    #   generator (optional, defaults to "manual")
    #   update (bool)

    import sys
    import os

    branch = sys.argv[1]

    branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

    if not os.path.isdir(branch_dir):
        raise Exception('Branch directory does not exist')


    config_dir = os.path.join(branch_dir, 'asdf', 'src', 'utils')
    sys.path.insert(0, config_dir)

    from config_utility import BranchConfig

    config = BranchConfig(branch=branch)


    # check mongodb connection
    if config.connection_status != 0:
        raise Exception("connection status error: {0}".format(
            config.connection_error))

    # -------------------------------------------------------------------------


    path = sys.argv[2]

    generator = sys.argv[3]

    if len(sys.argv) == 5:
        update = sys.argv[4]
    else:
        update = False

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
