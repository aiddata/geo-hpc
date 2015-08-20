<?php

set_time_limit(0);

switch ($_POST['call']) {

	case "requests":

		$search_type = $_POST['search_type'];
		$search_val = $_POST['search_val'];

		// init mongo
		$m = new MongoClient();
		$db = $m->selectDB('det');
		$col = $db->selectCollection('queue');

		if ($search_type == "email") {
			$query = array('email' => $search_val );

		} else if ($search_type == "id") {
			$query = array('_id' => new MongoId($search_val));

		} else {
			echo json_encode([]);
			break;
		}

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
