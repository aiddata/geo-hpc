

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
                    self.merge_lists[rid].append(('d2_data', extract_output, 
                                                  None))
                    if is_reliability_raster:
                        self.merge_lists[rid].append(
                            ('d2_data', extract_output[:-5]+"r.csv", None))


        return 1, extract_count, msr_count

