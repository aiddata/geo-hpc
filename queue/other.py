
    from extract_check import ExtractItem
    from msr_check import MSRItem


    def check_request(self, rid, request, extract=False):
        """check entire request object for cache
        """
        print "check_request"

        self.merge_lists[rid] = []
        extract_count = 0
        msr_count = 0

        msr_field_id = 1

        for name in sorted(request['d1_data'].keys()):
            print name

            data = request['d1_data'][name]

            data['resolution'] = self.msr_resolution
            data['version'] = self.msr_version

            # get hash
            data_hash = json_sha1_hash(data)

            msr_item = MSRItem(self.branch,
                               data["dataset"],
                               data_hash,
                               self.msr_version)

            # check if extract exists in queue and is completed
            msr_exists, msr_completed = msr_item.exists()


            if msr_completed == True:

                msr_ex_item = ExtractItem(self.branch,
                                          request["boundary"]["name"],
                                          data["dataset"],
                                          data_hash,
                                          "sum",
                                          True,
                                          "None"
                                          self.extract_version)

                msr_ex_exists, msr_ex_completed = msr_ex_item.exists()


                if not extract_completed:
                    extract_count += 1

                    if not extract_exists:
                        # add to extract queue
                        msr_ex_item.add_to_queue("msr")

            else:

                msr_count += 1
                extract_count += 1
                if not msr_exists:
                    # add to msr tracker
                    msr_item.add_to_queue("release")


            # add to merge list
            self.merge_lists[rid].append(
                ('d1_data', msr_ex_item.extract_path, msr_field_id))
            self.merge_lists[rid].append(
                ('d1_data', msr_ex_item.reliability_path, msr_field_id))

            msr_field_id += 1


        for name, data in request["d2_data"].iteritems():
            print name

            for i in data["files"]:

                for extract_type in data["options"]["extract_types"]:


                    extract_item = ExtractItem(self.branch,
                                               request["boundary"]["name"],
                                               data["name"],
                                               i["name"],
                                               extract_type,
                                               i["reliability"],
                                               data["temporal_type"]
                                               self.extract_version)

                    # check if extract exists in queue and is completed
                    extract_exists, extract_completed = extract_item.exists()

                    # incremenet count if extract is not completed
                    # (whether it exists in queue or not)
                    if extract_completed != True:
                        extract_count += 1

                        # add to extract queue if it does not already
                        # exist in queue
                        if not extract_exists:
                            extract_item.add_to_queue("external")



                    # add to merge list
                    self.merge_lists[rid].append(
                        ('d2_data', extract_item.extract_path, None))

                    if i["reliability"]:
                        self.merge_lists[rid].append(
                            ('d2_data', extract_item.reliability_path, None))



        return 1, extract_count, msr_count

