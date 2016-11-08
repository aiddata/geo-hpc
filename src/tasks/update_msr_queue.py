"""
update asdf/det msr tracker with all possible extract
combinations available from the asdf given specified depth/cut-off params
"""

# -----------------------------------------------------------------------------

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

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break

# -----------------------------------------------------------------------------


if config.connection_status != 0:
    print "mongodb connection error ({0})".format(config.connection_error)
    sys.exit()


import re
import time
import itertools
import numpy as np

import json
import hashlib
import pandas as pd

sys.path.insert(0, os.path.join(
    branch_dir, 'mean-surface-rasters', 'src'))

from msr_utility import CoreMSR

from check_releases import ReleaseTools


active_preambles = [i.lower() for i in config.release_iso3]

rtool_asdf = ReleaseTools()
rtool_asdf.set_asdf_releases(branch)
latest_releases = [i for i in rtool_asdf.get_latest_releases()
                   if i[0].split('_')[0] in active_preambles]

print latest_releases

# -------------------------------------


client = config.client
c_asdf = client.asdf.data
c_msr = client.asdf.msr
db_releases = client.releases

version = config.versions["mean-surface-rasters"]


# set active datasets that are not in config to inactive
c_asdf.update_many({
    'name': {'$nin': [i[0] for i in latest_releases]},
    'active': 1,
    'type': 'release'
}, {
   '$set': {'active': 0}
})


# -------------------------------------

# remove any items in queue for old datasets that have
# not yet been processed
delete_call = c_msr.delete_many({
    '$or': [
        {'dataset': {'$nin': [i[0] for i in latest_releases]}},
        {'options.version': {'$ne': version}}
    ],
    'status': 0,
    'priority': -1
})

deleted_count = delete_call.deleted_count
print '\n'
print (str(deleted_count) + ' unprocessed automated msr requests' +
       ' for outdated released have been removed.')


if len(sys.argv) == 3 and sys.argv[2] == "clean":
    sys.exit("update_msr_queue: cleaning complete")


# -------------------------------------

# filter combination generation settings

# minimum number of locations to accept when a filter
# combination has more than a single field
# (meaning: sum of fields in all filter categories > 1)
multi_cat_filter_min_len = 10

# max number of sector fields that can be in each filter
max_sector_count = 1

# max number of donor fields that can be in each filter
max_donor_count = 1

# minimum total aid to accept for a given filter
min_filter_aid_total = 0


# some other settings

# resolution of msr raster
msr_resolution = 0.05

# -------------------------------------


dataset_info = {}
for i in latest_releases:

    time_00 = int(time.time())

    ix = i[0]

    print '\n'
    print 'Running ' + str(ix)

    print 'Building filter combinations...'

    c_release_tmp = db_releases[ix]

    dataset_info[ix] = {
        'name': i[0],
        'base': i[2]
    }

    # unique sector list
    raw_distinct_sectors = c_release_tmp.distinct('ad_sector_names')

    tmp_sectors = []
    for j in raw_distinct_sectors:
        tmp_sectors += j.split('|')

    dataset_info[ix]['ad_sector_names'] = sorted(list(set(tmp_sectors)))
    # dataset_info[ix]['ad_sector_names'] = [x.encode('UTF8')
    #                                for x in sorted(list(set(tmp_sectors)))]

    # unique donor list
    raw_distinct_donors = c_release_tmp.distinct('donors')

    tmp_donors = []
    for j in raw_distinct_donors:
        tmp_donors += j.split('|')


    if len(tmp_donors) == 1:
        tmp_donors = []

    dataset_info[ix]['donors'] = sorted(list(set(tmp_donors)))
    # dataset_info[ix]['donors'] = [x.encode('UTF8')
    #                                 for x in sorted(list(set(tmp_donors)))]


    # ratio_list = itertools.product(
    #     range(max_sector_count), range(max_donor_count))


    ratio_list = []

    min_depth = min(max_sector_count, max_donor_count)

    for d in range(min_depth+2):
        ratio_list += [
            j
            for j in itertools.product(range(d), range(d))
            if j not in ratio_list
        ]
        # print d
        # print d-1
        # print ratio_list
        # print '---'


    if max_sector_count > max_donor_count:
        for d in range(min_depth+1, max_sector_count+1):
            # print d
            ratio_list += [
                j for j in itertools.product([d], range(min_depth+1))]

    elif max_donor_count > max_sector_count:
        for d in range(min_depth+1, max_donor_count+1):
            # print d
            ratio_list += [
                j for j in itertools.product(range(min_depth+1), [d])]


    # print '-----'
    # print ratio_list



    dataset_info[ix]['iter'] = itertools.chain.from_iterable(
        itertools.product(
            itertools.combinations(dataset_info[ix]['ad_sector_names'], j[0]),
            itertools.combinations(dataset_info[ix]['donors'], j[1]))
        for j in ratio_list)

    # print sum(1 for j in dataset_info[ix]['iter'])

    # for j in dataset_info[ix]['iter']:
    #     print j

    time_01 = int(time.time())

    # =========================================================================


    # print dataset_info[ix]

    print 'Generating jobs...'


    # create instance of CoreMSR class
    core = CoreMSR(config)

    # --------------------------------------------------
    # load project data

    dir_data = (dataset_info[ix]['base'] +'/data')

    df_merged = core.merge_data(dir_data, "project_id")

    df_prep = core.prep_data(df_merged)

    tmp_total_aid = sum(df_prep['split_dollars_pp'])

    time_02 = int(time.time())


    total_count = 0
    accept_count = 0
    add_count = 0

    for filter_fields in dataset_info[ix]['iter']:

        # tmp_sum += 1
        total_count += 1

        tmp_time = int(time.time())

        filter_sectors = list(filter_fields[0])
        filter_donors = list(filter_fields[1])

        # if filter_sectors == []:
        #     filter_sectors = ['All']

        # if filter_donors == []:
        #     filter_donors = ['All']

        # --------------------------------------------------
        # filters

        raw_filters = {
            'ad_sector_names': filter_sectors,
            'donors': filter_donors,
            'transaction_year': []
        }

        filters = {
            filter_field: raw_filters[filter_field]
            for filter_field in raw_filters
            if raw_filters[filter_field] and
            'All' not in raw_filters[filter_field]
        }


        df_filtered = core.filter_data(df_prep, filters)


        if not 'ad_sector_names' in filters.keys():
            sector_split_list = []
        else:
            sector_split_list = filters['ad_sector_names']

        if not 'donors' in filters.keys():
            donor_split_list = []
        else:
            donor_split_list = filters['donors']



        if (len(df_filtered) == 0 and
                len(sector_split_list) + len(donor_split_list) <= 1):
            print sector_split_list
            print donor_split_list
            # empty_sum += 1
            continue


        if (len(df_filtered) < multi_cat_filter_min_len and
                len(sector_split_list) + len(donor_split_list) > 1):
            # count_thresh_sum += 1
            continue

        # adjust aid based on ratio of sectors/donors in
        # filter to all sectors/donors listed for project

        try:
            df_filtered['adjusted_val'] = df_filtered.apply(
                lambda z: core.calc_adjusted_val(
                    z.split_dollars_pp, z.ad_sector_names, sector_split_list),
                axis=1)
        except:
            print df_filtered
            raise


        # print filter_sectors
        # print filter_donors
        # print '-'


        if sum(df_filtered['adjusted_val']) <= min_filter_aid_total: #sum(df_filtered['split_dollars_pp']):
            # aid_thresh_sum +=1
            continue

        # good_sum += 1

        # --------------------------------------------------

        # using filter, get project count and % total aid for release
        #   - % total aid is used to sort when checking for msr jobs

        filter_count = len(df_filtered)

        # filter_percentage = np.floor(
        #     100 * sum(df_filtered['adjusted_val']) /
        #     sum(df_filtered['split_dollars_pp']))
        filter_percentage = np.floor(
            1000 * 100 * sum(df_filtered['adjusted_val']) / tmp_total_aid ) / 1000

        # print '-'
        # print filter_sectors
        # print filter_donors
        # print filter_count
        # # print sum(df_filtered['adjusted_val'])
        # print filter_percentage


        # build msr object
        msr_object = {
                "dataset" : dataset_info[ix]['name'],
                "type" : "release",
                "version" : version,
                "resolution" : msr_resolution,
                "filters": filters
        }


        # get hash (sha-1) of filter object
        # - NOTE: DET queue (javascript: det-module>web>index.js)
        #         hashes are for only to make sure there are not
        #         duplicates within a request, so this hash does
        #         NOT need to match hashes from queue. This hash
        #         SHOULD match the hash generated by the queue
        #         processing (python: det-module>queue>cache.py)
        #         hash.

        def json_sha1_hash(hash_obj):
            hash_json = json.dumps(hash_obj,
                                   sort_keys = True,
                                   ensure_ascii = True,
                                   separators=(', ', ': '))
            hash_builder = hashlib.sha1()
            hash_builder.update(hash_json)
            hash_sha1 = hash_builder.hexdigest()
            return hash_sha1

        filter_hash = json_sha1_hash(msr_object)

        # build complete mongo doc
        #   - includes all info, same as msr from DET:
        #       research release, filters, etc.
        #   - indicate it was generated by auto and
        #       has correct flags (priority/status/etc.)
        mongo_doc = {
            "hash" : filter_hash,
            "options" : msr_object,
            "dataset" : dataset_info[ix]['name'],
            "status" : 0,
            "priority" : -1,
            "submit_time" : tmp_time,
            "update_time" : tmp_time,
            "classification": "auto-release",
            "count": filter_count,
            "percentage": filter_percentage
        }


        # add to msr tracker if hash does not exist
        exists = c_msr.update_one({'hash':mongo_doc['hash']},
                                  {'$setOnInsert': mongo_doc},
                                  upsert=True)

        accept_count += 1
        if exists.upserted_id != None:
            add_count += 1


    time_03 = int(time.time())
    print time_01 - time_00
    print time_02 - time_01
    print time_03 - time_02

    # print tmp_sum
    # print empty_sum
    # print count_thresh_sum
    # print aid_thresh_sum
    # print good_sum
    # print '--------------'
    # raise

    print ('Added ' + str(add_count) + ' items to msr queue (' +
           str(accept_count) + ' acceptable out of ' +
           str(total_count) + ' total possible).')







