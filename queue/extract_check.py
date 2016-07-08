

import os
import time
import pymongo


class ExtractItem():
    """stuff
    """
    def __init__(self, boundary, dataset, data,
                 extract_type, temporal_type, base):

        self.client = pymongo.MongoClient()

        self.c_extracts = self.client.asdf.extracts
        self.c_msr = self.client.asdf.msr


        self.boundary = boundary
        self.dataset = dataset
        self.data = data
        self.extract_type = extract_type

        self.temporal_type = temporal_type

        self.base = base

        self.extract_options = {
            "categorical": "c",
            "weighted_mean": "E",
            "weighted_count": "N",
            "weighted_sum": "S",
            "mean": "e",
            "count": "n",
            "sum": "s",
            "min": "m",
            "max": "x",
            "std": "d",

            "reliability": "r"

            # "median": "?"
            # "majority": "?"
            # "minority": "?"
            # "unique": "u"
            # "range": "?"

            # "percentile_?": "?"
            # "custom_?": "?"

            # "var": "v"
            # "mode": "?"


        }

        if self.extract_type in self.extract_options:
            self.extract_abbr = self.extract_options[self.extract_type ]
        else:
            raise Exception('invalid extract type')


    def __exists_in_db(self):
        """check if extract exists in extract queue db collection
        """
        check_data = {
            "boundary": self.boundary,
            "data": self.data,
            "extract_type": self.extract_type
        }

        # check db
        search = self.c_extracts.find_one(check_data)

        exists = not search is None
        status = search['status'] if exists else None
        return True, (exists, status)


    def __exists_in_file(self):
        """check if extract file exists

        also check reliability file when needed
        """
        # core basename for output file
        # does not include file type identifier
        #   (...e.ext for extracts and ...r.ext for reliability)
        #   or file extension
        partial_name = self.data
        if self.temporal_type == "None":
            partial_name = partial_name + "_"

        output_name = partial_name + self.extract_abbr + ".csv"

        # output file string without file type identifier
        # or file extension
        extract_path = os.path.join(
            self.base, self.boundary, "cache", self.dataset,
            self.extract_type, output_name)

        self.extract_path = extract_path


        extract_exists = os.path.isfile(extract_path)


        reliability_path = extract_path[:-5] + "r.csv"
        self.reliability_path = reliability_path

        reliability_exists = os.path.isfile(reliability_path)


        valid = False
        if (extract_exists and (not self.reliability or
                (self.reliability and reliability_exists))):
            valid = True

        return True, (valid, extract_exists, reliability_exists)


    def exists(self):
        """
        - check for extract file
        - check for reliability file if field is specified
        - check if extract is completed, waiting to be run, or
           encountered an error
        """
        db_info, db_tuple = self.__exists_in_db()
        (db_exists, db_status) = db_tuple

        file_info, file_tuple = self.__exists_in_file()
        (file_exists, file_extract_exists, file_reliability_exists) = file_tuple

        valid_exists = False
        valid_completed = False

        if db_exists:

            if db_status in [0,2,3]:
                valid_exists = True

            elif db_status == 1:

                if file_exists:
                    valid_exists = True
                    valid_completed = True

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed


    def add_to_queue(self, classification):
        """
        add extract item to det->extracts mongodb collection
        """
        ctime = int(time.time())

        query = {
            'data': self.data,
            'boundary': self.boundary,
            'extract_type': self.extract_type,
            'reliability': self.reliability
        }

        details = {
            'classification': classification,
            'generator': 'det',
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        full_insert = query.copy()
        full_insert.update(details)

        # check if exists
        search = self.c_extracts.find_one(query)

        if search is not None:
            if search['status'] == 0 and search['priority'] < 0:
                # update priority
                update = self.c_extracts.update(query,
                                                {'$set': {'priority': 0}})
        else:
            # insert full
            insert = self.c_extracts.insert(full_insert)

        return True, ctime

