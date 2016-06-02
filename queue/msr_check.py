

import os
import time
import pymongo


class MSRItem():
    """stuff
    """
    def __init__(self, dataset_name, msr_hash, selection, base):
        self.dataset_name = dataset_name
        self.msr_hash = msr_hash
        self.selection = selection

        self.base = base

        self.client = pymongo.MongoClient()
        self.c_msr = self.client.asdf.msr


    def __exists_in_db(self):

        check_data = {
            "dataset": self.dataset_name,
            "hash": self.msr_hash
        }

        # check db
        search = self.c_msr.find_one(check_data)

        exists = not search is None
        status = search['status'] if exists else None

        return True, (exists, status)


    def __exists_in_file(self):

        msr_base = os.path.join(
            self.base, self.dataset_name, self.msr_hash)

        raster_path = msr_base + '/raster.tif'
        geojson_path = msr_base + '/unique.geojson'
        summary_path = msr_base + '/summary.json'

        raster_exists = os.path.isfile(raster_path)
        geojson_exists = os.path.isfile(geojson_path)
        summary_exists = os.path.isfile(summary_path)

        msr_exists = raster_exists and geojson_exists and summary_exists

        return True, (msr_exists, raster_exists, geojson_exists, summary_exists)


    def exists(self):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered
           an error
        """
        db_info, db_tuple = self.__exists_in_db()
        (db_exists, db_status) = db_tuple

        file_info, file_tuple = self.__exists_in_file()
        (file_exists, file_raster_exists, file_geojson_exists, file_summary_exists) = file_tuple


        valid_exists = False
        valid_completed = False

        if db_exists:

            if db_status in [0,2]:
                valid_exists = True

            elif db_status== 1:

                if file_exists:
                    valid_exists = True
                    valid_completed = True

                # else:
                #     # remove from db
                #     self.c_msr.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed


    def add_to_queue(self):
        """add msr item to det->msr mongodb collection
        """
        ctime = int(time.time())

        query = {
            'hash': self.msr_hash,
            'dataset': self.dataset_name,
            'options': self.selection
        }

        details = {
            'classification': 'det-release',
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        full_insert = query.copy()
        full_insert.update(details)

        # check if exists
        search = self.c_msr.find_one(query)

        if search is not None:
             if search['status'] == 0 and search['priority'] < 0:
                # update priority
                update = self.c_msr.update(query,
                                           {'$set': {'priority': 0}})
        else:
            # insert full
            insert = self.c_msr.insert(full_insert)

        return True, ctime

