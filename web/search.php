<?php

set_time_limit(300);

$m = new MongoClient();


function is_clean_val($input) {
    // verify valid value
    if (!is_string($input) && !is_int($input) && !is_float($input)
        && !is_bool($input)
    ) {
        return False;
    }
    // verify string does not contain null bytes
    if (is_string($input) && strpos("\0", $input) !== False) {
        return False;
    }
    return True;
}

// https://stackoverflow.com/questions/173400/how-to-check-if-php-
//      array-is-associative-or-sequential/173479#173479
function is_assoc($arr) {
    return array_keys($arr) !== range(0, count($arr) - 1);
}

function is_clean_array($input) {
    // check if array
    if (!is_array($input)) {
        return False;
    }
    // check if associative array
    if (is_assoc($input)) {
        return False;
    }
    foreach ($input as $i) {
        if (!is_clean_val($i)) {
            return False;
        }
    }
    return True;
}


switch ($_POST['call']) {

// ============================================================================
// det:queue

    case "find_requests":
        /*
        generate request information for det status page

        post fields
            search_type : "id" or "email" or "status"
            search_val : id, email or status value

        returns
            det queue collection docs matching search_type and search_val
        */
        $search_type = $_POST['search_type'];
        $search_val = $_POST['search_val'];

        if (!is_clean_val($search_type) || !is_clean_val($search_val)) {
            echo json_encode('invalid inputs');
            break;
        }

        $db = $m->selectDB('det');
        $col = $db->selectCollection('queue');

        if ($search_type == "email") {
            $query = array('email' => $search_val);
            $cursor = $col->find($query);

        } else if ($search_type == "id") {
            $query = array('_id' => new MongoId($search_val));
            $cursor = $col->find($query);

        } else if ($search_type == "status") {
            $query = array('status' => $search_val);
            $cursor = $col->find($query);
            $cursor->sort(array('priority' => -1, 'submit_time' => 1));

        } else {
            echo json_encode([]);
            break;
        }

        $cursor->snapshot();

        $output = array();
        foreach ($cursor as $doc) {
            $output[] = $doc;
        }

        echo json_encode($output);
        break;


    case "update_request_status":
        /*
        update status of det request

        post fields
            rid: request doc's mongo id
            status: new status for request
            stage: field indicating request's progress with timestamp
            timestamp: value for field indicated by stage
        */
        $rid = $_POST['rid'];
        $status = $_POST['status'];
        $stage = $_POST['update'];
        $timestamp = $_POST['timestamp'];

        $valid_stages = array("prep_time", "process_time", "complete_time");

        if (!is_clean_val($rid) || !is_int($status)
            || !is_clean_val($stage) || !in_array($stage, $valid_stages)
            || !is_int($timestamp)
        ) {
            echo json_encode('invalid inputs');
            break;
        }

        $query = array('_id' => new MongoId($rid));

        $update = array();
        $update['status'] = $status;
        $update[$stage] = $timestamp;


        $db = $m->selectDB('det');
        $col = $db->selectCollection('queue');

        $cursor = $col->update($query, $update);

        $output = "good";


        // send email based on status (received / completed)
        // $mail_to = "sgoodman@aiddata.org";

        // $mail_subject = "Test";

        // $mail_message = "Your data request has been received and will be ";
        // $mail_message .= "processed soon. <br><br>";

        // $mail_headers = 'MIME-Version: 1.0' . "\r\n";
        // $mail_headers .= 'Content-type: text/html; charset=utf-8' . "\r\n";

        // $mail = mail($mail_to, $mail_subject, $mail_message, $mail_headers);


        echo json_encode($output);
        break;


    case "add_request":
        /*
        inserts request object as document in det->queue mongo db/collection
        sends email to user that made request [not working/enabled at the
            moment, may have moved this to python queue processing]

        post fields
            request : json string for request fields

        returns
            unique mongoid assigned to request
        */
        $request = json_decode($_POST['request']);

        // validate $request
        //

        $db = $m->selectDB('det');
        $col = $db->selectCollection('queue');

        // write request json to request db
        $col->insert($request);

        // get unique mongoid which will serve as request id
        $request_id = (string) $request->_id;

        // return request id
        echo json_encode(array($request_id));
        break;


// ============================================================================
// asdf:data

    case "find_boundaries":
        /*
        find and return all eligible boundaries

        post fields
            none

        returns
            json string representing dictionary where keys are boundary
            group names and values are lists of boundary docs for each
            boundary in group

        */
        $db = $m->selectDB('asdf');
        $col = $db->selectCollection('data');

        $query = array('type' => 'boundary', 'active' => 1);

        $fields = array(
            'name' => true,
            'title' => true,
            'description' => true,
            'source_link' => true,
            'options.group' => true,
            'options.group_class' => true,
            'base' => true,
            'resources.path' => true
        );

        $cursor = $col->find($query, $fields);
        $cursor->snapshot();

        $output = array();
        foreach ($cursor as $doc) {
            $output[$doc['options']['group']][] = $doc;
        }

        echo json_encode($output);
        break;


    case "find_relevant_datasets":
        /*
        find relevant datasets for specified boundary group

        post fields
            group : boundary group

        returns
            json object string with release datasets (d1)
            and raster datasets (d2)

            d1 and d2 objects contain key value pairs where key is
            dataset name and value is dataset doc
        */
        $group = $_POST['group'];

        if (!is_clean_val($group)) {
            echo json_encode('invalid inputs');
            break;
        }

        $db_asdf = $m->selectDB('asdf');
        $db_tracker = $m->selectDB('trackers');

        // get valid datasets from tracker
        $tracker_col = $db_tracker->selectCollection($group);

        $tracker_query = array('status' => 1);

        $tracker_fields = array(
            'name' => true,
        );

        $tracker_cursor = $tracker_col->find($tracker_query, $tracker_fields);
        $tracker_cursor->snapshot();

        // put tracker results into array
        $list = array();
        foreach ($tracker_cursor as $doc) {
            $list[] = $doc['name'];
        }

        // get data for datasets found in tracker
        $col = $db_asdf->selectCollection('data');

        $query = array(
            'name' => array('$in' => $list),
            'temporal.type' => array('$in' => array('year', 'None')),
            'type' => array('$in' => array('release', 'raster'))
        );

        $cursor = $col->find($query);
        $cursor->snapshot();


        $output = array('d1' => array(), 'd2' => array());

        foreach ($cursor as $doc) {

            if ($doc['type'] == "release") {


                // $doc['year_list'] = array();
                // $doc['sector_list'] = array();
                // $doc['donor_list'] = array();


                // get years from datapackage
                // $doc['year_list'] = range($doc['temporal'][0]['start'], $doc['temporal'][0]['end']);

                // placeholder for no year selection (only 'All')
                $doc['year_list'] = [];

                // get years based on min transaction_first and max
                // transaction_last
                //


                $db_releases = $m->selectDB('releases');
                $col_releases = $db_releases->$doc['name'];

                // $testhandle = fopen("/var/www/html/DET/test.csv", "w");
                // fwrite( $testhandle, json_encode($col_releases->find()) );


                $sectors = $col_releases->distinct('ad_sector_names');
                // $doc['sector_list'] = json_encode($sectors);
                for ($i=0; $i<count($sectors);$i++) {
                    if (strpos($sectors[$i], "|") !== false) {
                        $new = explode("|", $sectors[$i]);
                        $sectors[$i] = array_shift($new);
                        foreach ($new as $item) {
                            $sectors[] = $item;
                        }
                    }
                }
                // $doc['sector_list'] = sort(array_unique($sectors));
                $doc['sector_list'] = array_unique($sectors);
                sort($doc['sector_list']);

                $donors = $col_releases->distinct('donors');
                // $doc['donor_list'] = $donors;
                for ($i=0; $i<count($donors);$i++) {
                    if (strpos($donors[$i], "|") !== false) {
                        $new = explode("|", $donors[$i]);
                        $donors[$i] = array_shift($new);
                        foreach ($new as $item) {
                            $donors[] = $item;
                        }
                    }
                }
                // $doc['donor_list'] = sort(array_unique($donors));
                $doc['donor_list'] = array_unique($donors);
                sort($doc['donor_list']);

                $output['d1'][$doc['name']] = $doc;

            } else if ($doc['type'] == "raster") {
                $output['d2'][$doc['name']] = $doc;

            }

        }

        echo json_encode($output);
        break;


    case "find_dataset":
        /*
        find individual dataset from asdf>data using generic open ended query

        post fields
            query

        returns
            doc for match
        */
        $query = $_POST['query'];

        $db_asdf = $m->selectDB('asdf');
        $col = $db_asdf->selectCollection('data');

        $output = $col->findOne($query);

        echo json_encode($output);
        break;


    case "get_boundary_geojson":
        /*
        find and returns contents of simplified boundary geojson
        for web map

        post fields
            name : boundary

        returns
            simplified geojson as json
        */
        $name = $_POST['name'];

        if (!is_clean_val($name)) {
            echo json_encode('invalid inputs');
            break;
        }

        $db = $m->selectDB('asdf');
        $col = $db->selectCollection('data');


        $query = array('type' => 'boundary', 'name' => $name);

        $fields = array(
            'base' => true
        );

        $result = $col->findOne($query, $fields);

        $base = $result['base'];

        $file = $base . "/simplified.geojson";

        $output = file_get_contents($file);

        echo $output;
        break;


// ============================================================================
// asdf:extracts

    case "extracts":
        /*
        find or insert extract doc

        post fields
            method
            query
            insert
        */
        $method = $_POST['method'];

        $db = $m->selectDB('asdf');
        $col = $db->selectCollection('extracts');

        if ($method == 'find') {

            $query = json_decode($_POST['query']);

            // validate $query
            $valid_query_keys = array('boundary', 'raster', 'extract_type', 'reliability');
            foreach ($query as $k => $v) {
                if (!in_array($k, $valid_query_keys)
                    || $k == 'reliability' && !is_bool($v)
                    || $k !== 'reliability' && !is_clean_val($v)
                ){
                    echo json_encode('invalid inputs');
                    break;
                }
            }

            $cursor = $col->find($query);
            $output = $cursor->snapshot();

        } else if ($method == 'insert') {

            $insert = json_decode($_POST['insert']);

            // validate $insert
            //

            $col->insert($insert);
            $request_id = (string) $request->_id;
            $output = array($request_id);

        } else {
            $output = "invalid method";
        }

        echo json_encode($output);
        break;


// ============================================================================
// asdf:msr

    case "msr":
        /*
        find or insert msr doc

        post fields
            method
            query
            insert
        */
        $method = $_POST['method'];

        $db = $m->selectDB('asdf');
        $col = $db->selectCollection('msr');

        if ($method == 'find') {

            $query = json_decode($_POST['query']);

            // validate $query
            $valid_query_keys = array('dataset', 'hash');
            foreach ($query as $k => $v) {
                if (!in_array($k, $valid_query_keys) || !is_clean_val($v)){
                    echo json_encode('invalid inputs');
                    break;
                }
            }

            $cursor = $col->find($query);
            $output = $cursor->snapshot();

        } else if ($method == 'insert') {

            $insert = json_decode($_POST['insert']);

            // validate $insert
            //

            $col->insert($insert);
            $request_id = (string) $request->_id;
            $output = array($request_id);

        } else {
            $output = "invalid method";
        }

        echo json_encode($output);
        break;


// ============================================================================
// releases:any

    case "get_filter_count":
        /*
        get counts for release dataset based on filter

        post fields
            filter : fields and filters

        returns
            number of projects, locations, loc1?, loc2?
        */
        $filter = $_POST['filter'];

        // validate $filter
        foreach ($filter as $k => $v) {
            if (in_array($k, array("dataset", "type"))) {
                // dataset and type field must be str
                if (!is_clean_val($filter[$k])) {
                    echo json_encode('invalid inputs');
                    break;
                }
            } else {
                // all other fields must be arrays of strings
                if (!is_clean_array($filter[$k])) {
                    echo json_encode('invalid inputs');
                    break;
                }
            }
        }


        $db = $m->selectDB('releases');
        $col = $db->selectCollection($filter['dataset']);


        $regex_map = function($value) {
            $value = str_replace('(', '\(', $value);
            $value = str_replace(')', '\)', $value);
            return new MongoRegex("/.*" . $value . ".*/");
        };


        // get number of projects (filter)
        $project_query = array();

        if (!in_array("All", $filter['sectors'])) {
            $project_query['ad_sector_names'] = array(
                '$in' => array_map($regex_map, $filter['sectors'])
            );
        }

        if (!in_array("All", $filter['donors'])) {
            $project_query['donors'] = array(
                '$in' => array_map($regex_map, $filter['donors'])
            );
        }

        if (!in_array("All", $filter['years'])) {
            $project_query['transactions.transaction_year'] = array(
                '$in' => array_map('intval', $filter['years'])
            );
        }


        $project_cursor = $col->find($project_query);
        // $project_cursor->snapshot();

        $projects = $project_cursor->count();

        // get number of locations (filter non geocoded + filter geocoded
        // with locations unwind)

        $location_query_1 = $project_query;
        $location_query_1['is_geocoded'] = 0;

        $location_cursor_1 = $col->find($location_query_1);
        $location_count_1 = $location_cursor_1->count();


        $location_query_2 = $project_query;
        $location_query_2['is_geocoded'] = 1;

        $location_aggregate = array();
        $location_aggregate[] = array('$match' => $location_query_2);
        $location_aggregate[] = array(
            '$project' => array("project_id"=>1, 'locations'=>1)
        );
        $location_aggregate[] = array('$unwind' => '$locations');

        $location_cursor_2 = $col->aggregate($location_aggregate);
        $location_count_2 = count($location_cursor_2["result"]);


        $locations = $location_count_1 + $location_count_2;
        $output = array(
            "projects" => $projects,
            "locations" => $locations,
            "location_count_1" => $location_count_1,
            "location_count_2" => $location_count_2
        );

        echo json_encode($output);
        break;


}

?>
