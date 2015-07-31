<?php

set_time_limit(0);

switch ($_POST['call']) {

	case "geojson":

		$output = file_get_contents($_POST['file']);

		echo $output;
		break;

}

?>
