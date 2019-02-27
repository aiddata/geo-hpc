

import os
import time

import extract_utility



class ExtractItem():
    """check status of item in extract queue
    """
    def __init__(self, config, boundary, dataset, data,
                 extract_type, temporal_type, version):

        self.config = config

        self.client = self.config.client

        self.c_extracts = self.client.asdf.extracts
        self.c_msr = self.client.asdf.msr


        self.base = os.path.join(
            config.branch_dir, "outputs/extracts", version.replace('.', '_'))

        self.boundary = boundary
        self.dataset = dataset
        self.data = data
        self.extract_type = extract_type
        self.temporal_type = temporal_type
        self.version = version


        exo = extract_utility.ExtractObject()
        self.extract_options = exo._extract_options

        if self.extract_type not in self.extract_options:
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

        # print 'extract db info'
        # print exists
        # print status

        return exists, status


    def __exists_in_file(self):
        """check if extract file exists
        """
        temporal = self.temporal_type
        if temporal in ["None", None, "na", '']:
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

        extract_exists = os.path.isfile(extract_path) and os.stat(extract_path).st_size > 0

        # print 'extract file info'
        # print extract_path
        # print extract_exists

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
            'priority': 0,
            'update_time': ctime
        }

        full_insert = query.copy()
        full_insert.update(details)

        # check if exists
        search = self.c_extracts.find_one(query)

        if search is None:
            full_insert['status'] = 0
            full_insert['submit_time'] = ctime

            # insert full
            insert = self.c_extracts.insert(full_insert)

        # update priority if needed
        elif search is not None and search['generator'] != 'det':
                update = self.c_extracts.update(query,
                                                {'$set': details})


        return True, ctime

