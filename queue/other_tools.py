




class ExtractItem():
    """stuff
    """
    def __init__(self, boundary, raster, extract_type, reliability=False):

        self.boundary = boundary
        self.raster = raster
        self.extract_type = extract_type
        self.reliability = reliability

        self.post = Post()


    def exists(self, csv_path):
        """
        1) check if extract exists in extract queue,
           also check for reliability calc if field is specified
        2) check if extract is completed, waiting to be run, or
           encountered an error
        """
        check_data = {
            "boundary": boundary,
            "raster": raster,
            "extract_type": extract_type,
            "reliability": reliability
        }

        post_data = {
            'call': 'update_extracts',
            'method': 'find',
            'query': check_data
        }

        try:
            r = self.post.send(post_data)

            if r['status'] == 'error':
                warnings.warn(r['error'])

        except Exception as e:
            warnings.warn(e)
            return "error"


        search = r['data']

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:
            print search[0]

            if search[0]['status'] in [0,2,3]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                extract_exists = os.path.isfile(csv_path)

                reliability_path = csv_path[:-5] + "r.csv"

                if (extract_exists and (not reliability or
                        (reliability and os.path.isfile(reliability_path)))):
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_extracts.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed


    def add_to_queue(self, boundary, raster, extract_type, reliability,
                       classification):
        """
        add extract item to det->extracts mongodb collection
        """
        ctime = int(time.time())

        query = {
            'raster': raster,
            'boundary': boundary,
            'reliability': reliability,
            'extract_type': extract_type,
        }

        # check if extract exists

        # if it does
        #     if status == 0 and priority < 0
        #         set priority = 0

        # else
        #     insert full extract


        insert = {
            'classification': classification,
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }


        post_data = {
            'call': 'update_extracts',
            'method': 'insert',
            'insert': json.dumps(insert)
        }

        try:
            r = self.post(post_data)

            if r['status'] == 'success':
                return True, ctime
            else:
                return False, ctime

        except:
            return False, None













class MSRItem():
    """stuff
    """
    def __init__(self, dataset_name, msr_hash):

        self.dataset_name = dataset_name
        self.msr_hash = msr_hash

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
            "std": "d"

            # "median": "?"
            # "majority": "?"
            # "minority": "?"
            # "unique": "u"
            # "range": "r"

            # "percentile_?": "?"
            # "custom_?": "?"

            # "var": "v"
            # "mode": "?"
        }


    def exists(self, dataset_name, msr_hash):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered an error
        """
        print "exists_in_msr_tracker"

        check_data = {"dataset": self.dataset_name, "hash": self.msr_hash}

        # check db
        search = self.c_msr.find(check_data)

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:

            if search[0]['status'] in [0,2]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                raster_path = ('/sciclone/aiddata10/REU/data/rasters/' +
                               'internal/msr/' + self.dataset_name +'/'+ self.msr_hash +
                               '/raster.asc')

                msr_exists = os.path.isfile(raster_path)

                if msr_exists:
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_msr.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed



    def add_to_queue(self, selection, msr_hash):
        """add msr item to det->msr mongodb collection
        """
        print "add_to_msr_tracker"

        ctime = int(time.time())

        insert = {
            'hash': msr_hash,
            'dataset': selection['dataset'],
            'options': selection,

            'classification': 'det-release',
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        self.c_msr.insert(insert)






# =============================================================================
# =============================================================================
# =============================================================================





    def extract_exists(self, boundary, raster, extract_type, reliability,
                       csv_path):
        """
        1) check if extract exists in extract queue
           run redundancy check on actual extract file and delete extract
           queue entry if file is missing
           also check for reliability calc if field is specified
        2) check if extract is completed, waiting to be run, or
           encountered an error
        """
        check_data = {
            "boundary": boundary,
            "raster": raster,
            "extract_type": extract_type,
            "reliability": reliability
        }

        # check db
        search = self.c_extracts.find(check_data)

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:
            print search[0]

            if search[0]['status'] in [0,2,3]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                extract_exists = os.path.isfile(csv_path)

                reliability_path = csv_path[:-5] + "r.csv"

                if (extract_exists and (not reliability or
                        (reliability and os.path.isfile(reliability_path)))):
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_extracts.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed


    def update_extract(self, boundary, raster, extract_type, reliability,
                       classification):
        """
        add extract item to det->extracts mongodb collection
        """
        ctime = int(time.time())

        query = {
            'raster': raster,
            'boundary': boundary,
            'reliability': reliability,
            'extract_type': extract_type,
        }

        # check if extract exists

        # if it does
        #     if status == 0 and priority < 0
        #         set priority = 0

        # else
        #     insert full extract


        insert = {
            'classification': classification,
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }


        post_data = {
            'call': 'update_extracts',
            'method': 'insert',
            'insert': json.dumps(insert)
        }

        try:
            r = self.post(post_data)

            if r['status'] == 'success':
                return True, ctime
            else:
                return False, ctime

        except:
            return False, None





    def msr_exists(self, dataset_name, msr_hash):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered an error
        """
        print "exists_in_msr_tracker"

        check_data = {"dataset": dataset_name, "hash": msr_hash}

        # check db
        search = self.c_msr.find(check_data)

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:

            if search[0]['status'] in [0,2]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                raster_path = ('/sciclone/aiddata10/REU/data/rasters/' +
                               'internal/msr/' + dataset_name +'/'+ msr_hash +
                               '/raster.asc')

                msr_exists = os.path.isfile(raster_path)

                if msr_exists:
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_msr.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed



    def add_to_msr_tracker(self, selection, msr_hash):
        """add msr item to det->msr mongodb collection
        """
        print "add_to_msr_tracker"

        ctime = int(time.time())

        insert = {
            'hash': msr_hash,
            'dataset': selection['dataset'],
            'options': selection,

            'classification': 'det-release',
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        self.c_msr.insert(insert)

