<?php

set_time_limit(0);

switch ($_POST['call']) {

	case "geojson":

		$output = file_get_contents($_POST['file']);

		echo $output;
		break;

	case "request":

		$request = json_decode($_POST['request']);

		// init mongo
		$m = new MongoClient();
		$db = $m->selectDB('det');
		$col = $db->selectCollection('queue');

		// write request json to request db
		$col->insert($request);

		// get unique mongoid which will serve as request id
		$request_id = (string) $request->_id;


		// // generate and send email
		// $mail_to = $request->email;
		
		// $mail_subject = "AidData - Data Extraction Tool: Request #".$request_id." Received";
		
		// $mail_message = "Your data request has been received and will be processed soon. <br><br>";
		// $mail_message .= "You can check the status of you request and access the data when it completes through the following link: devlabs.aiddata.wm.edu/DET/stuff/#".$request_id."<br><br>";
		// $mail_message .= "An additional email will be sent when your request has bee completed.";
		
		// $mail_headers = 'MIME-Version: 1.0' . "\r\n";
		// $mail_headers .= 'Content-type: text/html; charset=iso-8859-1' . "\r\n";
		
		// $mail = mail($mail_to, $mail_subject, $mail_message, $mail_headers);


		// return request id
		echo json_encode(array($request_id));
		break;
}

?>
