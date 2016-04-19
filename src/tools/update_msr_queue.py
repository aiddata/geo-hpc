#

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


import re
import time
import pymongo
import itertools
import numpy as np

import json
import hashlib
import pandas as pd

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))),
    'mean-surface-rasters', 'src'))

from msr_utility import CoreMSR

from check_releases import ReleaseTools


rtool_asdf = ReleaseTools()
rtool_asdf.set_asdf_releases("develop")
latest_releases = rtool_asdf.get_latest_releases()


# -------------------------------------


client = pymongo.MongoClient(config.server)
c_msr = client[config.msr_db].msr
db_releases = client[config.release_db]

version = config.versions["mean-surface-rasters"]


# -------------------------------------

print version

f2 = c_msr.find({
    'version': {'$ne': version},
    'status': 0,
    'priority': -1
}).count()
print f2

f4 = c_msr.find({
    '$or': [
        {'version': {'$ne': version}}
    ],
    'status': 0,
    'priority': -1
}).count()
print f4

f5 = c_msr.find({
    '$or': [
        {'dataset': {'$nin': [i[0] for i in latest_releases]}},
        {'version': {'$ne': version}}
    ],
    'status': 0,
    'priority': -1
}).count()
print f5

sys.exit("!!!!!")


# remove any items in queue for old datasets that have
# not yet been processed
delete_call = c_msr.delete_many({
    '$or': [
        {'dataset': {'$nin': [i[0] for i in latest_releases]}},
        {'version': {'$ne': version}}
    ],
    'status': 0,
    'priority': -1
})

deleted_count = delete_call.deleted_count
print '\n'
print (str(deleted_count) + ' unprocessed automated msr requests' +
       ' for outdated released have been removed.')


# -------------------------------------


dataset_info = {}
for i in latest_releases:

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

    dataset_info[ix]['sectors'] = sorted(list(set(tmp_sectors)))
    # dataset_info[ix]['sectors'] = [x.encode('UTF8')
    #                                for x in sorted(list(set(tmp_sectors)))]

    # unique donor list
    raw_distinct_donors = c_release_tmp.distinct('donors')

    tmp_donors = []
    for j in raw_distinct_donors:
        tmp_donors += j.split('|')

    dataset_info[ix]['donors'] = sorted(list(set(tmp_donors)))
    # dataset_info[ix]['donors'] = [x.encode('UTF8')
    #                                 for x in sorted(list(set(tmp_donors)))]


    max_sector_count = 1
    max_donor_count = 1

    ratio_list = itertools.product(
        range(max_sector_count), range(max_donor_count))


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
            ratio_list += [j for j in itertools.product([d],
                                                        range(min_depth+1))]

    elif max_donor_count > max_sector_count:
        for d in range(min_depth+1, max_donor_count+1):
            # print d
            ratio_list += [j for j in itertools.product(range(min_depth+1),
                                                        [d])]


    # print '-----'
    # print ratio_list



    dataset_info[ix]['iter'] = itertools.chain.from_iterable(
        itertools.product(
            itertools.combinations(dataset_info[ix]['sectors'], j[0]),
            itertools.combinations(dataset_info[ix]['donors'], j[1]))
        for j in ratio_list)

    # print sum(1 for j in dataset_info[i]['iter'])

    # for j in dataset_info[i]['iter']:
    #     print j


    # =========================================================================


    # print dataset_info[ix]

    print 'Generating jobs...'


    # create instance of CoreMSR class
    core = CoreMSR()

    # --------------------------------------------------
    # load project data

    dir_data = (dataset_info[ix]['base'] +'/data')

    df_merged = core.prep_data(dir_data)

    tmp_total_aid = sum(df_merged['split_dollars_pp'])


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
            'years': []
        }

        filters = {
            filter_field: raw_filters[filter_field]
            for filter_field in raw_filters
            if raw_filters[filter_field] and
            'All' not in raw_filters[filter_field]
        }


        df_filtered = core.filter_data(df_merged, filters)


        if not 'ad_sector_names' in filters.keys():
            sector_split_list = []
        else:
            sector_split_list = filters['ad_sector_names']

        if not 'donors' in filters.keys():
            donor_split_list = []
        else:
            donor_split_list = filters['donors']



        if len(df_filtered) == 0:
            if len(sector_split_list) + len(donor_split_list) <= 1:
                print sector_split_list
                print donor_split_list
            # empty_sum += 1
            continue

        if (len(df_filtered) < 10 and
                len(sector_split_list) + len(donor_split_list) > 1):
            # count_thresh_sum += 1
            continue

        # adjust aid based on ratio of sectors/donors in
        # filter to all sectors/donors listed for project

        try:
            df_filtered['adjusted_aid'] = df_filtered.apply(
                lambda z: core.calc_adjusted_aid(
                    z.split_dollars_pp, z.ad_sector_names, z.donors,
                    sector_split_list, donor_split_list), axis=1)
        except:
            print df_filtered
            raise


        # print filter_sectors
        # print filter_donors
        # print '-'

        if sum(df_filtered['adjusted_aid']) == 0: #sum(df_filtered['split_dollars_pp']):
            # aid_thresh_sum +=1
            continue

        # good_sum += 1

        # --------------------------------------------------

        # using filter, get project count and % total aid for release
        #   - % total aid is used to sort when checking for msr jobs

        filter_count = len(df_filtered)

        # filter_percentage = np.floor(
        #     100 * sum(df_filtered['adjusted_aid']) /
        #     sum(df_filtered['split_dollars_pp']))
        filter_percentage = np.floor(
            100 * 100 * sum(df_filtered['adjusted_aid']) / tmp_total_aid ) / 100

        # print '-'
        # print filter_sectors
        # print filter_donors
        # print filter_count
        # # print sum(df_filtered['adjusted_aid'])
        # print filter_percentage


        # build msr object
        msr_object = {
                "dataset" : dataset_info[ix]['name'],
                "type" : "release",
                "version" : version,
                "resolution" : 0.05,
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
            # "job" : [],
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







