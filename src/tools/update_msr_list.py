# 

# --------------------------------------------------

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


# --------------------------------------------------

import time
import pymongo
import itertools
import numpy as np



client = pymongo.MongoClient(config.server)

asdf = client[config.asdf_db].data
msr = client[config.det_db].msr
releases = client.releases

# get names of all research releases from asdf
all_releases = [(i['name'], i['base']) for i in asdf.find({'type':'release'}, {'name':1, 'base':1})]


# preambles from name which identify country/group data pertains to
all_preambles = [i[0].split('_')[0] for i in all_releases]

# duplicates based on matching preambles
# means there are multiple versions
duplicate_preambles = [i for i in set(all_preambles) if all_preambles.count(i) > 1]

# unique list of latest dataset names
# initialized here with only datasets that have a single version
latest_releases = [i for i in all_releases if not i[0].startswith(tuple(duplicate_preambles))]


# iterate over each group of conflicting datasets based on preamble
for i in duplicate_preambles:

    # get full names using preamble
    conflict_releases = [k for k in all_releases if k[0].startswith(i)]

    latest_version = None

    # find which dataset is latest version
    for j in conflict_releases:

        tmp_version = float(j[0].split('_')[-1][1:])

        if latest_version == None:
            latest_version = tmp_version

        elif tmp_version > latest_version:
            latest_version = tmp_version

    # add latest version dataset to final list
    latest_releases += [j for j in conflict_releases if j[0].endswith(str(latest_version))]


dataset_info = {}
for i in latest_releases:

    ix = i[0]

    print 'Building filter combinations for: ' + str(ix)

    tmp_collection = releases[ix]

    dataset_info[ix] = {
        'name': i[0],
        'base': i[1]
    }

    # unique sector list
    raw_distinct_sectors = tmp_collection.distinct('ad_sector_names')

    tmp_sectors = []
    for j in raw_distinct_sectors:
        tmp_sectors += j.split('|')

    dataset_info[ix]['sectors'] = sorted(list(set(tmp_sectors)))
    # dataset_info[ix]['sectors'] = [x.encode('UTF8') for x in sorted(list(set(tmp_sectors)))]

    # unique donor list
    raw_distinct_donors = tmp_collection.distinct('donors')

    tmp_donors = []
    for j in raw_distinct_donors:
        tmp_donors += j.split('|')
   
    dataset_info[ix]['donors'] = sorted(list(set(tmp_donors)))
    # dataset_info[ix]['donors'] = [x.encode('UTF8') for x in sorted(list(set(tmp_donors)))]



    max_sector_count = 1
    max_donor_count = 1

    ratio_list = itertools.product(range(max_sector_count), range(max_donor_count))


    ratio_list = []

    min_depth = min(max_sector_count, max_donor_count)

    for d in range(min_depth+2):
        ratio_list += [j for j in itertools.product(range(d), range(d)) if j not in ratio_list]
        # print d
        # print d-1
        # print ratio_list
        # print '---'


    if max_sector_count > max_donor_count:
        for d in range(min_depth+1, max_sector_count+1):
            # print d
            ratio_list += [j for j in itertools.product([d], range(min_depth+1))]

    elif max_donor_count > max_sector_count:
        for d in range(min_depth+1, max_donor_count+1):
            # print d
            ratio_list += [j for j in itertools.product(range(min_depth+1), [d])]


    # print '-----'
    # print ratio_list



    dataset_info[ix]['iter'] = itertools.chain.from_iterable(itertools.product(itertools.combinations(dataset_info[ix]['sectors'], j[0]), itertools.combinations(dataset_info[ix]['donors'], j[1])) for j in ratio_list)

    # print sum(1 for j in dataset_info[i]['iter'])

    # for j in dataset_info[i]['iter']:
    #     print j


# ====================================================================================

import json
import hashlib
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'mean-surface-rasters', 'src'))
from msr_utility import CoreMSR


# print dataset_info
# tot_sum = 0
print '\n'
for ix in dataset_info.keys():
    
    print 'Generating jobs for: ' + dataset_info[ix]['name']


    # create instance of CoreMSR class
    core = CoreMSR()

    # --------------------------------------------------
    # load project data

    # dir_data = dir_file+"/countries/"+country+"/versions/"+country+"_"+str(data_version)+"/data"
    dir_data = dataset_info[ix]['base'] +'/'+ os.path.basename(dataset_info[ix]['base']) +'/data'

    merged = core.merge_data(dir_data, "project_id", (core.code_field_1, core.code_field_2, "project_location_id"), core.only_geocoded)
    
    # merged['ad_sector_names'] = [x.encode('UTF8') for x in merged['ad_sector_names']]
    # merged['donors'] = [x.encode('UTF8') for x in merged['donors']]

    # --------------------------------------------------
    # misc data prep

    # get location count for each project
    merged['ones'] = (pd.Series(np.ones(len(merged)))).values

    # get project location count
    grouped_location_count = merged.groupby('project_id')['ones'].sum()


    # create new empty dataframe
    df_location_count = pd.DataFrame()

    # add location count series to dataframe
    df_location_count['location_count'] = grouped_location_count

    # add project_id field
    df_location_count['project_id'] = df_location_count.index

    # merge location count back into data
    merged = merged.merge(df_location_count, on='project_id')

    # aid field value split evenly across all project locations based on location count
    merged[core.aid_field].fillna(0, inplace=True)
    merged['split_dollars_pp'] = (merged[core.aid_field] / merged.location_count)


    tmp_total_aid = sum(merged['split_dollars_pp'])

    # print len(merged)
    # print tmp_total_aid
    # print '---'

    # tmp_sum = 0
    # good_sum = 0
    # count_thresh_sum = 0
    # aid_thresh_sum = 0
    # empty_sum = 0
    total_count = 0
    accept_count = 0
    add_count = 0

    for filter_fields in dataset_info[ix]['iter']:
        # tmp_sum += 1
        total_count += 1

        tmp_time = int(time.time())

        filter_sectors = list(filter_fields[0])
        filter_donors = list(filter_fields[1])

        if filter_sectors == []:
            filter_sectors = ['All']

        if filter_donors == []:
            filter_donors = ['All']

        # using filter, get project count and % total aid for release
        #   - % total aid is used to sort when checking for msr jobs
        #   - make sure filter using mongo release collections is same 
        #     what we get from current msr job using release csv'same
        #   - should probably update msr jobs to use mongo releases now

        
        tmp_merged = merged.copy(deep=True)

      
        # --------------------------------------------------
        # filters

        # filter years
        # 

        # filter sectors and donors
        if filter_donors == ['All'] and filter_sectors != ['All']:
            filtered = tmp_merged.loc[tmp_merged['ad_sector_names'].str.contains('('+'|'.join(filter_sectors)+')')].copy(deep=True)

        elif filter_donors != ['All'] and filter_sectors == ['All']:
            filtered = tmp_merged.loc[tmp_merged['donors'].str.contains('('+'|'.join(filter_donors)+')')].copy(deep=True)

        elif filter_donors != ['All'] and filter_sectors != ['All']:
            filtered = tmp_merged.loc[(tmp_merged['ad_sector_names'].str.contains('('+'|'.join(filter_sectors)+')')) & (tmp_merged['donors'].str.contains('('+'|'.join(filter_donors)+')'))].copy(deep=True)

        else:
            filtered = tmp_merged.copy(deep=True)
         

        if len(filtered) == 0:
            # empty_sum += 1
            continue

        if len(filtered) < 10: #0.05 * len(merged):
            # count_thresh_sum += 1
            continue

        # adjust aid based on ratio of sectors/donors in filter to all sectors/donors listed for project
        filtered['adjusted_aid'] = filtered.apply(lambda z: core.adjust_aid(z.split_dollars_pp, z.ad_sector_names, z.donors, filter_sectors, filter_donors), axis=1)
        

        # print filter_sectors
        # print filter_donors
        # print '-'

        if sum(filtered['adjusted_aid']) == 0: #sum(filtered['split_dollars_pp']):
            # aid_thresh_sum +=1
            continue

        # good_sum += 1

        # --------------------------------------------------

        filter_count = len(filtered)

        # filter_percentage = np.floor(100 * sum(filtered['adjusted_aid']) / sum(filtered['split_dollars_pp']))
        filter_percentage = np.floor( 100 * 100 * sum(filtered['adjusted_aid']) / tmp_total_aid ) / 100

        # print '-'
        # print filter_sectors
        # print filter_donors
        # print filter_count
        # # print sum(filtered['adjusted_aid'])
        # print filter_percentage


        # build filter object
        filter_object = {
                "donors" : filter_donors,
                "sectors" : filter_sectors,
                "resolution" : 0.05,
                "years" : ["All"],
                "version" : 0.1,
                "dataset" : dataset_info[ix]['name'],
                "type" : "release",
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
            hash_json = json.dumps(hash_obj, sort_keys = True, ensure_ascii = True, separators=(',', ':'))
            hash_builder = hashlib.sha1()
            hash_builder.update(hash_json)
            hash_sha1 = hash_builder.hexdigest()
            return hash_sha1

        filter_hash = json_sha1_hash(filter_object)

        # build complete mongo doc
        #   - includes all info, same as msr from DET: research release, filters, etc.
        #   - indicate it was generated by auto and has correct flags (priority/status/etc.)
        mongo_doc = {
            "hash" : filter_hash,
            "options" : filter_object,
            "dataset" : dataset_info[ix]['name'],
            "status" : 0,
            "priority" : -1,
            "job" : [],
            "submit_time" : tmp_time,
            "update_time" : tmp_time,

            "classification": "auto-release",
            "count": filter_count,
            "percentage": filter_percentage
        }


        # add to msr tracker if hash does not exist
        exists = msr.update_one({'hash':mongo_doc['hash']}, {'$setOnInsert': mongo_doc}, upsert=True)
        
        accept_count += 1
        if exists.upserted_id != None:
            add_count += 1

    # print tmp_sum
    # print empty_sum
    # print count_thresh_sum
    # print aid_thresh_sum
    # print good_sum
    # print '--------------'
    # tot_sum += tmp_sum
    # raise
    print 'Added ' + str(add_count) + ' items to msr queue (' + str(accept_count) + ' acceptable out of ' + str(total_count) + ' total possible).'


# print tot_sum




# remove any items in queue for old datasets that have not yet been processed
delete_call = msr.delete_many({'dataset': {'$nin': [i[0] for i in latest_releases]}, 'status': 0, 'priority': -1}) 
deleted_count = delete_call.deleted_count
print '\n' 
print str(deleted_count) + ' unprocessed automated msr requests for outdated released have been removed.'
