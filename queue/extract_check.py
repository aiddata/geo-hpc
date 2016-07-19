

import os
import time


class ExtractItem():
    """check status of item in extract queue
    """
    def __init__(self, client, base, boundary, dataset, data,
                 extract_type, temporal_type, version):

        self.client = client

        self.c_extracts = self.client.asdf.extracts
        self.c_msr = self.client.asdf.msr

        self.base = base

        self.boundary = boundary
        self.dataset = dataset
        self.data = data
        self.extract_type = extract_type
        self.temporal_type = temporal_type
        self.version = version

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
            # "std": "d",

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

        self.extract_path = None


    def __exists_in_db(self):
        """check if extract exists in extract queue db collection
        """
        check_data = {
            "boundary": self.boundary,
            "data": self.data,
            "extract_type": self.extract_type,
            "version": self.version
        }

        search = self.c_extracts.find_one(check_data)

        exists = search is not None
        status = search['status'] if exists else None
        return exists, status


    def __exists_in_file(self):
        """check if extract file exists
        """
        temporal = self.temporal_type
        if temporal in ["None", None, "na"]:
            temporal = "na"

        # full file name
        output_name = '{0}.{1}.{2}.csv'.format(self.dataset,
                                               temporal,
                                               self.extract_type)

        # absolute output path
        extract_path = os.path.join(
            self.base, self.boundary, "cache", self.dataset, output_name)

        # need as attribute so it can be added to merge list
        # outside class instance
        self.extract_path = extract_path


        extract_exists = os.path.isfile(extract_path)

        return extract_exists


    def exists(self):
        """check if extract exists and status

        - check for extract db entry and file
        - check if extract is completed, waiting to be run, or
           encountered an error
        """
        db_exists, db_status = self.__exists_in_db()

        file_exists = self.__exists_in_file()

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
        """add extract item to asdf->extracts mongodb collection
        """
        ctime = int(time.time())

        query = {
            'boundary': self.boundary,
            'data': self.data,
            'extract_type': self.extract_type,
            'version': self.version
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

