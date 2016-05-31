
class MSRItem():
    """stuff
    """
    def __init__(self, dataset_name, msr_hash, version):

        self.dataset_name = dataset_name
        self.msr_hash = msr_hash

        self.base = "/sciclone/aiddata10/REU/extracts/"


    def __exists_in_db(self):

        check_data = {
            "dataset": self.dataset_name,
            "hash": self.msr_hash
        }

        # check db
        search = self.c_msr.find_one(check_data)

        db_exists = search.count() > 0

        return True, (db_exists, search['status'])


    def __exists_in_file(self):

        msr_base = os.path.join(
            self.base, self.dataset_name, self.msr_hash)

        raster_path = csv_path + '/raster.tif'
        geojson_path = csv_path + '/unique.geojson'
        summary_path = csv_path + '/summary.json'

        raster_exists = os.path.isfile(raster_path)
        geojson_exists = os.path.isfile(geojson_path)
        summary_exists = os.path.isfile(summary_path)

        msr_exists = raster_exists and geojson_exists and summary_exists

        return True, (msr_exists, raster_exists, geojson_exists, summary_exists)


    def exists(self, dataset_name, msr_hash):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered
           an error
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
                               'internal/msr/' + self.dataset_name +'/'+
                               self.msr_hash + '/raster.asc')

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


# // // ------------------------------------
# // // asdf:msr

# // /**
# // find or insert msr doc

# // post fields
# //     method
# //     query
# //     insert
# // */
# // function update_msr() {
# //     global $output, $m;

# //     $method = $_POST['method'];

# //     $db = $m->selectDB('asdf');
# //     $col = $db->selectCollection('msr');

# //     if ($method == 'find') {

# //         $query = json_decode($_POST['query']);

# //         // validate $query
# //         $valid_query_keys = array('dataset', 'hash');
# //         foreach ($query as $k => $v) {
# //             if (!in_array($k, $valid_query_keys) || !is_clean_val($v)) {
# //                 $output->error('invalid inputs')->send();
# //                 return 0;
# //             }
# //         }

# //         $cursor = $col->find($query);
# //         $result = iterator_to_array($cursor, false);
# //         $output->send($result);

# //     } else if ($method == 'insert') {

# //         $insert = json_decode($_POST['insert']);

# //         // validate $insert
# //         //

# //         $col->insert($insert);
# //         $request_id = (string) $request->_id;

# //         $output->send($request_id);

# //     } else {
# //         $output->error('invalid method')->send();

# //     }
# //     return 0;
# // }
