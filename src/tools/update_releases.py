

# -----------------------------------------------------------------------------

import sys
import os

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

# -----------------------------------------------------------------------------


import errno
import pymongo
import zipfile
import re
from check_releases import ReleaseTools

sys.path.insert(0, os.path.dirname(config_dir))
import add_release


repo_dir = branch_dir + "/public_datasets/geocoded"

data_dir = "/sciclone/aiddata10/REU/data/releases"


try:
    os.makedirs(data_dir)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise

# -----------------------------------------------------------------------------
# find latest version of releases from repo which match current format

all_repo_releases = os.listdir(repo_dir)

print all_repo_releases

modern_str = ".*AIMS_GeocodedResearchRelease_Level1_v.*\.zip"
modern_expr = re.compile(modern_str)
modern_repo_releases = [(i[:-4],) for i in all_repo_releases
                        if modern_expr.match(i)]

print modern_repo_releases

rtool_repo = ReleaseTools()
rtool_repo.set_user_releases(modern_repo_releases)
latest_repo_releases = [i[0] for i in rtool_repo.get_latest_releases()]

sys.exit("!!!")

print rtool_repo.get_latest_releases()
print latest_repo_releases

# -----------------------------------------------------------------------------
# unzip any latest releases which doe not exist in data dir

existing_data_releases = os.listdir(data_dir)

new_releases = [i for i in latest_repo_releases
                if i not in existing_data_releases]

print new_releases

for i in new_releases:

    zpath = repo_dir +"/"+ i

    zobj = zipfile.Zipfile(zpath)

    zobj.extractall(data_dir)


# -----------------------------------------------------------------------------

# rtool_data = ReleaseTools()
# rtool_data.set_dir_releases(data_dir)
# latest_releases = [i[0] for i in rtool_data.get_latest_releases()]

outdated_releases = [i for i in os.listdir(data_dir)
                     if i not in latest_repo_releases]

print outdated_releases


client = pymongo.MongoClient(config.server)
asdf = client[config.asdf_db].data

# check if already in asdf
# run add_release to add if needed
for i in latest_repo_releases:

    ipath = data_dir +"/"+ i

    find_latest = asdf.find_one({"base": ipath})
    latest_exists = find_latest != None

    if not latest_exists:
        add_release.main([branch, ipath, "auto"])


# mark as inactive in asdf
for i in outdated_releases:

    ipath = data_dir +"/"+ i

    update_outdated = asdf.update_one({"base": ipath}, {"$set": {"active": 0}})

