<?php
/**
handle post requests for data extraction tool web page
*/

set_time_limit(300);

$m = new MongoClient();

$output = new Output();

if (empty($_POST['call'])) {
    $output->error('no call provided')->send();
    exit;
}


$call = $_POST['call'];

if (!function_exists($call)) {
    $output->error('invalid call')->send();
    exit;
}

// run specified function
call_user_func($call);

//  valid $call options:
//      "add_request"
//      "get_requests"
//      "get_boundaries"
//      "get_relevant_datasets"
//      "get_boundary_geojson"
//      "get_filter_count"


// ===========================================================================


/**
return post requests

returns array containing:
    status (success, error)
    error (error info, if status == "error")
    params (from original post)
    data (return data)
*/
class Output {
    private $output = [
        'status'=> 'success',
        'error'=> null,
        'params'=> null,
        'data'=> null
    ];

    /**
    add post params to response if they exist
    */
    public function __construct() {
        if (!empty($_POST)) {
            $this->output['params'] = $_POST;
        }
    }

    /**
    add error status and information to response
    */
    public function error($error = null) {
        $this->output['status'] = 'error';
        $this->output['error'] = $error;
        return $this;
    }

    /**
    add data to response and send response
    */
    public function send($data = null) {
        $this->output['data'] = $data;
        echo json_encode($this->output);
    }
}


// ===========================================================================
// functions for basic type validation of data being input into
// mongo collections

function is_clean_val($input) {
    if (!is_string($input) && !is_numeric($input) && !is_bool($input)) {
        return False;
    }
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
    if (!is_array($input)) {
        return False;
    }
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


// ===========================================================================
// functions for post requests

/**
generate request information for det status page

db:col = det:queue

post fields
    search_type : "id" or "email"
    search_val : id or email  value

returns
    det queue collection docs matching search_type and search_val
*/
function get_requests() {
    global $output, $m;

    $search_type = $_POST['search_type'];
    $search_val = $_POST['search_val'];


    if (!is_clean_val($search_type) || !is_clean_val($search_val)) {
        $output->error('invalid inputs')->send([]);
        return 0;
    }

    $db = $m->selectDB('det');
    $col = $db->selectCollection('queue');

    if ($search_type == "email") {
        $query = array('email' => $search_val);
        $cursor = $col->find($query);

    } else if ($search_type == "id") {
        try {
            $query = array('_id' => new MongoId($search_val));
            $cursor = $col->find($query);
        } catch (Exception $e) {
            $output->error('invalid id')->send([]);
            return 0;
        }

    } else {
        $output->error('invalid search type')->send([]);
        return 0;
    }

    $result = iterator_to_array($cursor, false);
    $output->send($result);
    return 0;
}


/**
inserts request object as document in det->queue mongo db/collection

db:col = det:queue

post fields
    request : json string for request fields

returns
    unique mongoid assigned to request
*/
function add_request() {
    global $output, $m;

    $request = json_decode($_POST['request']);

    // validate $request
    //

    $db = $m->selectDB('det');
    $col = $db->selectCollection('queue');

    // write request json to request db
    $col->insert($request);

    // get unique mongoid which will serve as request id
    $request_id = (string) $request->_id;

    $result =  [
        'request_id' => $request_id,
        'request' => $request
    ];

    $output->send($result);
    return 0;
}


/**
find and return all eligible boundaries

db:col = asdf:data

post fields
    None

returns
    json string representing dictionary where keys are boundary
    group names and values are lists of boundary docs for each
    boundary in group

*/
function get_boundaries() {
    global $output, $m;

    $db = $m->selectDB('asdf');
    $col = $db->selectCollection('data');

    $query = array('type' => 'boundary', 'active' => 1);

    // $fields = array(
    //     'base' => true,
    //     'name' => true,
    //     'title' => true,
    //     'description' => true,
    //     'version' => true,
    //     'options.group' => true,
    //     'options.group_class' => true,
    //     'options.group_title' => true,
    //     'resources.path' => true,
    //     'extras' => true
    // );
    // $cursor = $col->find($query, $fields);


    $cursor = $col->find($query);
    //// $cursor->snapshot();

    $result = array();
    foreach ($cursor as $doc) {
        $result[$doc['options']['group']][] = $doc;
    }

    $output->send($result);
    return 0;
}


/**
find relevant datasets for specified boundary group

db:col = asdf:data

post fields
    group : boundary group

returns
    json object string with release datasets (d1)
    and raster datasets (d2)

    d1 and d2 objects contain key value pairs where key is
    dataset name and value is dataset doc
*/
function get_relevant_datasets() {
    global $output, $m;

    $group = $_POST['group'];

    if (!is_clean_val($group)) {
        $output->error('invalid inputs')->send();
        return 0;
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
    //$tracker_cursor->snapshot();

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
        'type' => array('$in' => array('release', 'raster')),
        'active' => 1
    );

    $cursor = $col->find($query);
    //$cursor->snapshot();


    $result = array();

    foreach ($cursor as $doc) {

        if ($doc['type'] == "release") {

            $db_releases = $m->selectDB('releases');
            $col_releases = $db_releases->$doc['name'];

            // $testhandle = fopen("/var/www/html/DET/test.csv", "w");
            // fwrite( $testhandle, json_encode($col_releases->find()) );

            $release_query = [
                'is_geocoded' => 1
            ];


            // // get years from datapackage
            // $tmp_format = $doc['temporal']['format'];
            // $tmp_start = 1900 + strptime($doc['temporal']['start'], $tmp_format)['tm_year'];
            // $tmp_end = 1900 + strptime($doc['temporal']['end'], $tmp_format)['tm_year'];
            // $doc['years'] = range($tmp_start, $tmp_end);

            // get years from transactions
            $years = $col_releases->distinct('transactions.transaction_year', $release_query);
            $doc['years'] = array_unique($years);
            sort($doc['years']);


            // get sectors
            $sectors = $col_releases->distinct('ad_sector_names', $release_query);
            for ($i=0; $i<count($sectors);$i++) {
                if (strpos($sectors[$i], "|") !== false) {
                    $new = explode("|", $sectors[$i]);
                    $sectors[$i] = array_shift($new);
                    foreach ($new as $item) {
                        $sectors[] = $item;
                    }
                }
            }
            $doc['ad_sector_names'] = array_unique($sectors);
            sort($doc['ad_sector_names']);


            // get donors
            $donors = $col_releases->distinct('donors', $release_query);
            $doc['donors'] = array_unique($donors);
            sort($doc['donors']);


            $result[] = $doc;

        } else if ($doc['type'] == "raster") {
            $result[] = $doc;

        }

    }

    $output->send($result);
    return 0;
}


/**
find and returns contents of simplified boundary geojson
for web map

db:col = asdf:data

post fields
    name : boundary

returns
    simplified geojson as json
*/
function get_boundary_geojson() {
    global $output, $m;

    $name = $_POST['name'];

    if (!is_clean_val($name)) {
        $output->error('invalid inputs')->send();
        return 0;
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

    $result = file_get_contents($file);

    $output->send($result);
    return 0;
}


/**
get counts for release dataset based on filter

db:col = releases:*

post fields
    filter : fields and filters

returns
    number of projects, locations, and array assoc array containing
    distinct values for each search field based on current search params
    loc1?, loc2?
*/
function get_filter_count() {
    global $output, $m;

    $filter = $_POST['filter'];

    // validate $filter
    foreach ($filter as $k => $v) {
        if (in_array($k, array("dataset"))) {
            // dataset and type field must be str
            if (!is_clean_val($filter[$k])) {
                $output->error('invalid inputs')->send();
                return 0;
            }
        } else {
            // // all other fields must be arrays of strings
            // if (!is_clean_array($filter[$k])) {
            //     $output->error('invalid inputs')->send();
            //     return 0;
            // }
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
    $project_query = [
        'is_geocoded' => 1
    ];

    $distinct_fields = [];

    foreach ($filter['filters'] as $k => $v) {
        $tmp_project_query = [
            'is_geocoded' => 1
        ];

        if (!in_array("All", $v)) {
            if ($k == 'years') {
                $tmp_search = array(
                    '$in' => array_merge(
                        array_map('intval', $filter['filters']['years']),
                        array_map('strval', $filter['filters']['years'])
                    )
                );
                $project_query['transactions.transaction_year'] = $tmp_search;
                $tmp_project_query['transactions.transaction_year'] = $tmp_search;
            } else {
                $tmp_search = array(
                    '$in' => array_map($regex_map, $v)
                );
                $project_query[$k] = $tmp_search;
                $tmp_project_query[$k] = $tmp_search;
            }
        }

        foreach ($filter['filters'] as $kx => $vx) {
            if ($kx != $k) {

                if ($kx == "years") {
                    // placeholder query
                    $tmp_distinct = $col->distinct('transactions.transaction_year', $tmp_project_query);

                } else {
                    $tmp_distinct = $col->distinct($kx, $tmp_project_query);
                }

                // split on pipe and remove duplicaties
                for ($i=0; $i<count($tmp_distinct);$i++) {
                    if (strpos($tmp_distinct[$i], "|") !== false) {
                        $new = explode("|", $tmp_distinct[$i]);
                        $tmp_distinct[$i] = array_shift($new);
                        foreach ($new as $item) {
                            $tmp_distinct[] = $item;
                        }
                    }
                }
                $tmp_distinct = array_unique($tmp_distinct);
                sort($tmp_distinct);

                if (!array_key_exists($kx, $distinct_fields)){
                    $distinct_fields[$kx] = $tmp_distinct;
                } else {
                    $distinct_fields[$kx] = array_intersect($distinct_fields[$kx], $tmp_distinct);
                }
            }
        }

    }


    $project_cursor = $col->find($project_query);
    //// $project_cursor->snapshot();

    $projects = $project_cursor->count();

    // get number of locations (filter non geocoded + filter geocoded
    // with locations unwind)

    // $location_query_1 = $project_query;
    // $location_query_1['is_geocoded'] = 0;

    // $location_cursor_1 = $col->find($location_query_1);
    // $location_count_1 = $location_cursor_1->count();


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

    $locations = $location_count_2;
    // $locations = $location_count_1 + $location_count_2;
    $result = array(
        "projects" => $projects,
        "locations" => $locations,
        "distinct" => $distinct_fields
        // "location_count_1" => $location_count_1,
        // "location_count_2" => $location_count_2
    );

    $output->send($result);
    return 0;
}


?>
