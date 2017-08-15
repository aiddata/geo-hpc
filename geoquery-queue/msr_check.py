

import os
import time


class MSRItem():
    """check status of item in msr queue
    """
    def __init__(self, client, base, data_hash, selection):
        self.client = client
        self.c_msr = self.client.asdf.msr

        self.base = base

        self.data_hash = data_hash

        self.selection = selection
        self.dataset_name = selection['dataset']


    def __exists_in_db(self):

        check_data = {
            "dataset": self.dataset_name,
            "hash": self.data_hash
        }

        # check db
        search = self.c_msr.find_one(check_data)

        exists = not search is None
        status = search['status'] if exists else None

        return exists, status


    def __exists_in_file(self):

        msr_base = os.path.join(
            self.base, self.dataset_name, self.data_hash)

        raster_path = msr_base + '/raster.tif'
        geojson_path = msr_base + '/unique.geojson'
        summary_path = msr_base + '/summary.json'

        raster_exists = os.path.isfile(raster_path) and os.stat(raster_path).st_size > 0
        geojson_exists = os.path.isfile(geojson_path) and os.stat(geojson_path).st_size > 0
        summary_exists = os.path.isfile(summary_path) and os.stat(summary_path).st_size > 0

        msr_exists = raster_exists and geojson_exists and summary_exists

        return msr_exists, raster_exists, geojson_exists, summary_exists


    def exists(self):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered
           an error
        """
        db_exists, db_status = self.__exists_in_db()

        (file_exists, file_raster_exists,
         file_geojson_exists, file_summary_exists) = self.__exists_in_file()


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
            'hash': self.data_hash,
            'dataset': self.dataset_name,
            'options': self.selection
        }

        details = {
            'classification': 'det-release',
            'priority': 0,
            'update_time': ctime
        }

        full_insert = query.copy()
        full_insert.update(details)

        # check if exists
        search = self.c_msr.find_one(query)

        if search is not None:
             # if search['status'] == 0 and search['priority'] < 0:

            # update priority
            update = self.c_msr.update(query,
                                       {'$set': details})
        else:

            full_insert['status'] = 0
            full_insert['submit_time'] = ctime

            # insert full
            insert = self.c_msr.insert(full_insert)

        return True, ctime

