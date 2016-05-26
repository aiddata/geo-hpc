




class ExtractItem():
    """stuff
    """
    def __init__(self, boundary, raster_dataset, raster, extract_type, reliability=False):

        self.boundary = boundary
        self.dataset = dataset
        self.raster = raster
        self.extract_type = extract_type
        self.reliability = reliability

        self.post = Post()

        self.base = "/sciclone/aiddata10/REU/extracts/"

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

        if self.extract_type in self.extract_options:
            self.extract_abbr = self.extract_options
        else:
            raise Exception('invalid extract type')


    def __exists_in_db(self):
        """check if extract exists in extract queue db collection
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
                return False, "handled post error"

            search = r['data']
            db_exists = search.count() > 0
            return True, (db_exists, r['data']['status'])

        except Exception as e:
            warnings.warn(e)
            return False, "unhandled post error"


    def __exists_in_file(self):

        csv_path =

        # core basename for output file
        # does not include file type identifier
        #   (...e.ext for extracts and ...r.ext for reliability)
        #   or file extension
        partial_name = self.raster
        if data["temporal_type"] == "None":
            partial_name = partial_name + "_"

        output_name = partial_name + self.extract_abbr + ".csv"

        # output file string without file type identifier
        # or file extension
        extract_output = os.path.join(
            self.base, self.boundary, "cache", self.dataset,
            self.extract_type, output_name)


        extract_exists = os.path.isfile(csv_path)



        reliability_path = csv_path[:-5] + "r.csv"

        reliability_exists = os.path.isfile(reliability_path)

        if (extract_exists and (not reliability or
                (reliability and reliability_exists))):
            valid_exists = True
            valid_completed = True





    def exists(self, csv_path):
        """
        - check for extract file
        - check for reliability file if field is specified
        - check if extract is completed, waiting to be run, or
           encountered an error
        """


        ec, exists, status = self.__exists_in_db()


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







# =============================================================================
# =============================================================================
# =============================================================================







    def check_request(self, rid, request, extract=False):
        """check entire request object for cache
        """
        print "check_request"

        self.merge_lists[rid] = []
        extract_count = 0
        msr_count = 0

        msr_field_id = 1

        for name in sorted(request['d1_data'].keys()):
            data = request['d1_data'][name]

        # for name, data in request['d1_data'].iteritems():

            print name

            data['resolution'] = self.msr_resolution
            data['version'] = self.msr_version

            # get hash
            data_hash = json_sha1_hash(data)

            msr_extract_type = "sum"
            msr_extract_output = ("/sciclone/aiddata10/REU/extracts/" +
                                  request["boundary"]["name"] + "/cache/" +
                                  data['dataset'] +"/" + msr_extract_type +
                                  "/" + data_hash + "_" +
                                  self.extract_options[msr_extract_type] +
                                  ".csv")

            # check if msr exists in tracker and is completed
            msr_exists, msr_completed = self.msr_exists(data['dataset'],
                                                        data_hash)

            print "MSR STATE:" + str(msr_completed)

            if msr_completed == True:

                # check if extract for msr exists in queue and is completed
                extract_exists, extract_completed = self.extract_exists(
                    request["boundary"]["name"], data['dataset']+"_"+data_hash,
                    msr_extract_type, True, msr_extract_output)

                if not extract_completed:
                    extract_count += 1

                    if not extract_exists:
                        # add to extract queue
                        self.update_extract(
                            request["boundary"]["name"],
                            data['dataset']+"_"+data_hash,
                            msr_extract_type, True, "msr")

            else:

                msr_count += 1
                extract_count += 1

                if not msr_exists:
                    # add to msr tracker
                    self.add_to_msr_tracker(data, data_hash)


            # add to merge list
            self.merge_lists[rid].append(
                ('d1_data', msr_extract_output, msr_field_id))
            self.merge_lists[rid].append(
                ('d1_data', msr_extract_output[:-5]+"r.csv", msr_field_id))

            msr_field_id += 1


        for name, data in request["d2_data"].iteritems():
            print name

            for i in data["files"]:

                df_name = i["name"]
                raster_path = data["base"] +"/"+ i["path"]
                is_reliability_raster = i["reliability"]

                for extract_type in data["options"]["extract_types"]:

                    # core basename for output file
                    # does not include file type identifier
                    #   (...e.ext for extracts and ...r.ext for reliability)
                    #   or file extension
                    if data["temporal_type"] == "None":
                        output_name = df_name + "_"
                    else:
                        output_name = df_name

                    # output file string without file type identifier
                    # or file extension
                    base_output = ("/sciclone/aiddata10/REU/extracts/" +
                                   request["boundary"]["name"] + "/cache/" +
                                   data["name"] + "/" + extract_type + "/" +
                                   output_name)

                    extract_output = (base_output +
                                      self.extract_options[extract_type] +
                                      ".csv")

                    # check if extract exists in queue and is completed
                    extract_exists, extract_completed = self.extract_exists(
                        request["boundary"]["name"], df_name, extract_type,
                        is_reliability_raster, extract_output)

                    # incremenet count if extract is not completed
                    # (whether it exists in queue or not)
                    if extract_completed != True:
                        extract_count += 1

                        # add to extract queue if it does not already
                        # exist in queue
                        if not extract_exists:
                            self.update_extract(
                                request['boundary']['name'], i['name'],
                                extract_type, is_reliability_raster,
                                "external")


                    # add to merge list
                    self.merge_lists[rid].append(('d2_data', extract_output, None))
                    if is_reliability_raster:
                        self.merge_lists[rid].append(
                            ('d2_data', extract_output[:-5]+"r.csv", None))


        return 1, extract_count, msr_count

