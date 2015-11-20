<?php

set_time_limit(0);

switch ($_POST['call']) {
	
	case "boundaries":

		// init mongo
		$m = new MongoClient();
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

				// get years based on min transaction_first and max transaction_last
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


		  //   	// load datapackage for sectors/donors
		  //   	$rdp = json_decode(file_get_contents($doc['base'].'/'.basename($doc['base']).'/datapackage.json'), true);

		  //   	foreach ($rdp['sectors_names_list'] as $k => $sector_string) {
		  //   		foreach (explode('|', $sector_string['name']) as $sector) {

		  //   			$sector = trim($sector);
		  //   			if (!in_array($sector, $doc['sector_list'])) {
				// 	    	$doc['sector_list'][] = $sector;
				// 		}
				// 	}
		  //   	}
				// sort($doc['sector_list']);

		  //   	foreach ($rdp['donors_list'] as $k => $donor_string) {
		  //   		foreach (explode('|', $donor_string['name']) as $donor) {

		  //   			$donor = trim($donor);
		  //   			if (!in_array($donor, $doc['donor_list'])) {
				// 	    	$doc['donor_list'][] = $donor;
				// 		}
				// 	}
		  //   	}
				// sort($doc['donor_list']);
		    	




		    	$output['d1'][$doc['name']] = $doc;

		    } else if ($doc['type'] == "raster") {
		    	$output['d2'][$doc['name']] = $doc;

		    }

		}

		echo json_encode($output);
		break;


	case "filter_count":

		$filter = $_POST['filter'];

		// init mongo
		$m = new MongoClient();
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
			$project_query['ad_sector_names'] = array('$in' => array_map($regex_map, $filter['sectors']));
		}

		if (!in_array("All", $filter['donors'])) {
			$project_query['donors'] = array('$in' => array_map($regex_map, $filter['donors']));
		}

		if (!in_array("All", $filter['years'])) {
			$project_query['transactions.transaction_year'] = array('$in' => array_map('intval', $filter['years']));
		}


		$project_cursor = $col->find($project_query);
		// $project_cursor->snapshot();

		$projects = $project_cursor->count();




		// get number of locations (filter non geocoded + filter geocoded with locations unwind)

		$location_query_1 = $project_query;
		$location_query_1['is_geocoded'] = 0;

		$location_cursor_1 = $col->find($location_query_1);
		$location_count_1 = $location_cursor_1->count();


		$location_query_2 = $project_query;
		$location_query_2['is_geocoded'] = 1;

		$location_aggregate = array();
		$location_aggregate[] = array('$match' => $location_query_2);
		$location_aggregate[] = array('$project' => array("project_id"=>1, 'locations'=>1));
		$location_aggregate[] = array('$unwind' => '$locations');

		$location_cursor_2 = $col->aggregate($location_aggregate);
		$location_count_2 = count($location_cursor_2["result"]);



		$locations = $location_count_1 + $location_count_2;
		$output = array("projects" => $projects, "locations" => $locations, "location_count_1" => $location_count_1, "location_count_2" => $location_count_2 );

		echo json_encode($output);
		break;		

}

?>
