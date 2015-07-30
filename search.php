<?php

set_time_limit(0);

switch ($_POST['call']) {
	

	case "boundaries":

		// init mongo
		$m = new MongoClient();
		$db = $m->selectDB('asdf');
		$col = $db->selectCollection('data');


		$query = array('type' => 'boundary');

		$fields = array(
			'name' => true, 
			'title' => true,
			'short' => true,
			'source_link' => true,
			'options.group' => true,
			'options.group_class' => true,
			'base' => true,
			'resources.path' => true
		);

		$cursor = $col->find($query, $fields);
		$cursor->snapshot();

		$actuals = array();
		$all = array();

		foreach ($cursor as $doc) {
		    
		    $all[$doc['options']['group']][] = $doc;

		    if ($doc['options']['group_class'] == 'actual') {
		    	$actuals[$doc['options']['group']] = $doc['name'];
		    }
		}

		$output = array();

		foreach ($actuals as $group => $name) {
			if (count($all[$group]) > 1 ) {
				$output[$group] = $all[$group];
			} else if (count($all[$group]) == 1) {
				$output['single_datasets'][] = $all[$group][0];
			}
		}

		echo json_encode($output);
		break;



	case "datasets":

		$group = $_POST['group'];

		// init mongo
		$m = new MongoClient();
		$db = $m->selectDB('asdf');

		// get valid datasets from tracker
		$tracker_col = $db->selectCollection($group);

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
		$col = $db->selectCollection('data');



		$query = array('name' => array('$in' => $list));

		$cursor = $col->find($query);
		$cursor->snapshot();


		$output = array();

		foreach ($cursor as $doc) {
		    
		    $output[] = $doc;

		}

		echo json_encode($output);
		break;

}


	// // return list of fields for selected country
	// case "fields":
	// 	$database = $_POST['country'];
	// 	$collection = "complete";

	// 	$m = new MongoClient();
	// 	$db = $m->selectDB($database);
	// 	$col = $db->$collection;
	// 	$cursor = $col->find();

	//     $first = true;
	//     foreach ($cursor as $item) {
	//         if ( $first == true ){
	//     	    $data = (array) $item;
	//             $out = array_keys( $data );
	//             array_shift($out);
	//             $first = false;
	//         }
	//     }

	// 	echo json_encode($out);
	// 	break;

	// // return options for specific field
	// case "options":
	// 	$database = $_POST['country'];
	// 	$field = $_POST['field'];
	// 	$collection = "complete";

	// 	$m = new MongoClient();
	// 	$db = $m->selectDB($database);
	// 	$col = $db->$collection;
	// 	$data = $col->distinct($field);

	// 	// initial split
	// 	for ($i=0; $i<count($data);$i++) {
	// 		if (strpos($data[$i], "|") !== false) {
	// 			$new = explode("|", $data[$i]);
	// 			$data[$i] = array_shift($new);
	// 			foreach ($new as $item) {
	// 				$data[] = $item;
	// 			}
	// 		}
	// 	}

	// 	$out = array_unique($data);
	// 	echo json_encode($out);
	// 	break;








	// 	// fwrite( $testhandle, json_encode($query) );
	// 	$cursor = $col->aggregate($query);
	// 	// fwrite( $testhandle2, json_encode($cursor) );

	// 	//build csv if query produced results
	// 	if ( count($cursor["result"]) > 0 ) {

	// 		$c = 0;
	// 		foreach ($cursor["result"] as $item) {
	//     	    $row = (array) $item;
	//     	    $array_values = array_values($row); 

	//             if ($request == 2 || $request == 0 || $aggregate == "geography") {
		    	    
	// 	    	    // get rid of mongo _id field
	//            		array_shift($row);

	//            		// manage csv header
	// 			 	if ($c == 0) {
	// 			 		$array_keys = array_keys($row);
	// 			    	fputcsv($file, $array_keys);
	// 			    	$c = 1;
	// 			 	}

	// 			 	// get rid of extra mongo _id field
	// 			 	array_shift($array_values);
				 	

	//            	} 





?>
