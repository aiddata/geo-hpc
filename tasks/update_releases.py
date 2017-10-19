"""Checks on releases and runs ingest to update

Args
    branch (required)
    update (optional): use to define update method when running add_release.
                       defaults to "full if left blank
"""

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)
config.test_connection()

# -----------------------------------------------------------------------------


if config.connection_status != 0:
    print "mongodb connection error ({0})".format(config.connection_error)
    sys.exit()


import errno
# import pymongo
import zipfile
import re

from check_releases import ReleaseTools

sys.path.insert(0, os.path.join(os.path.dirname(config_dir), 'ingest'))
import add_release


repo_dir = config.source_dir + "/public_datasets/geocoded"

data_dir = os.path.join(config.data_root, "data/releases")


try:
    os.makedirs(data_dir)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise

# -----------------------------------------------------------------------------
# find all versions of releases from repo which match current format

all_repo_releases = os.listdir(repo_dir)


modern_str = ".*_GeocodedResearchRelease_Level1_v\d\.\d\.\d"
modern_expr_dir = re.compile(modern_str)
modern_expr_zip = re.compile(modern_str+"\.zip")
modern_repo_dirnames = [i[:-4] for i in all_repo_releases
                        if modern_expr_zip.match(i)]


# -----------------------------------------------------------------------------
# unzip any modern releases which do not exist in data dir

existing_data_dirnames = os.listdir(data_dir)

new_repo_dirnames = [i for i in modern_repo_dirnames
                     if i not in existing_data_dirnames]


print "extracting new geocoded datasets..."
for i in new_repo_dirnames:

    zpath = repo_dir +"/"+ i + ".zip"
    print "\t extracting: {0}".format(zpath)
    zobj = zipfile.ZipFile(zpath)
    zobj.extractall(data_dir)


# -----------------------------------------------------------------------------

rtool_repo = ReleaseTools()
rtool_repo.set_dir_releases(data_dir)
latest_data_dirnames = [os.path.basename(i[2]) for i in rtool_repo.get_latest_releases()
                        if modern_expr_dir.match(os.path.basename(i[2]))]


# rtool_data = ReleaseTools()
# rtool_data.set_dir_releases(data_dir)
# latest_releases = [i[0] for i in rtool_data.get_latest_releases()]

outdated_data_dirnames = [i for i in os.listdir(data_dir)
                          if i not in latest_data_dirnames]


client = config.client
c_asdf = client.asdf.data

version = config.versions["release-ingest"]


if len(sys.argv) >= 3:
    update = sys.argv[2]
else:
    update = "full"


if len(sys.argv) >= 4:
    dry_run = sys.argv[3]

    if dry_run in ["false", "False", "0", "None", "none", "no"]:
        dry_run = False

    dry_run = bool(dry_run)
    if dry_run:
        print "running dry run"

else:
    dry_run = False



# check if already in asdf
# run add_release to add if needed
for i in latest_data_dirnames:

    ipath = data_dir +"/"+ i

    find_latest = c_asdf.find_one({
        "base": ipath,
        "asdf.version": version
    })
    latest_exists = find_latest != None

    if not latest_exists:
        print "adding " + i + "..."
        add_release_instance = add_release
        add_release_instance.run(path=ipath, config=config,
                                 generator="auto", update=update,
                                 dry_run=dry_run)


# mark as inactive in asdf
for i in outdated_data_dirnames:

    ipath = data_dir +"/"+ i

    if not dry_run:
        update_outdated = c_asdf.update_one(
            {"base": ipath},
            {"$set": {"active": 0}}
        )

