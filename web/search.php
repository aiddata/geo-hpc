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

			$output[$group] = $all[$group];

			// if (count($all[$group]) > 1 ) {
			// 	$output[$group] = $all[$group];
			// } else if (count($all[$group]) == 1) {
			// 	$output['single_datasets'][] = $all[$group][0];
			// }
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

		$query = array(
						'name' => array('$in' => $list), 
						'temporal.type' => array('$in' => array('year', 'None')),
						'type' => 'raster'
		);

		$cursor = $col->find($query);
		$cursor->snapshot();


		$output = array();

		foreach ($cursor as $doc) {
		    
		    $output[] = $doc;

		}

		echo json_encode($output);
		break;

}

?>
