"""
example usage:

cd /sciclone/aiddata10/geo/master/source
python geo-hpc/ingest/dataset_ingest.py master raster geo-datasets/gpw/gpw_v3_count_raster_ingest.json manual partial True
"""
import sys
import os
import json

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)


# -------------------------------------------------------------------------


branch = sys.argv[1]

from config_utility import BranchConfig

config = BranchConfig(branch=branch)

# check mongodb connection
if config.connection_status != 0:
    raise Exception("connection status error: {0}".format(
        config.connection_error))


# -------------------------------------------------------------------------


dataset_type = sys.argv[2]

if dataset_type == "raster":
    from add_raster import run

elif dataset_type == "release":
    from add_release import run

elif dataset_type == "boundary":
    from add_boundary import run

elif dataset_type == "gadm":
    from add_gadm import run

else:
    raise Exception("invalid dataset type ({0})".format(dataset_type))


# -------------------------------------------------------------------------


path = sys.argv[3]


# get inputs
if dataset_type in ["raster", "boundary"]:

    if not os.path.isfile(path):
        raise Exception("invalid ingest json file path")

    data = json.load(open(path, 'r'))

    if not 'base' in data:
        raise Exception("base path missing from ingest json")

    if not os.path.exists(data['base']):
        raise Exception('specified base path does not exist')


elif dataset_type in ["release", "gadm"] and not os.path.isdir(path):
    raise Exception("invalid ingest directory path")


# if not os.path.isdir(data['base']):
    # raise Exception('base path is not a directory')

# check / update permissions for everything in base path
#
# i think desired result is that everything in aiddata10/geo only has
# user writes, and has same user. but since individual users process the
# data (initially goes to aiddata10/pre_geo) we would need root to change
# owner. this would not be an issue except that ingest sometimes produces
# new files (e.g., boundary geojsons) which cannot be done when a different
# user runs the ingest and group write is disabled
#
# possible solutions:
#   - get root to change owner every time [not going to happen]
#   - same person always runs final data processing job [extra time/space]
#   - enable group write [prefer to keep restricted access to final data files]
#   - ingest user copies files instead of moving files [extra time/space, probably less than rerunning though]
#


# -------------------------------------------------------------------------

generator = sys.argv[4]

if len(sys.argv) >= 6:
    update = sys.argv[5]
else:
    update = False

if len(sys.argv) >= 7:
    dry_run = sys.argv[6]
else:
    dry_run = False


run(path=path, config=config, generator=generator,
    update=update, dry_run=dry_run)

