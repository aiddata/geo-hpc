<!DOCTYPE html>
<html>

<head>
    <meta charset="UTF-8">
    <title>AidData DET Status Page</title> 

    <link href="http://netdna.bootstrapcdn.com/font-awesome/4.0.3/css/font-awesome.css" rel="stylesheet">

    <link href='http://fonts.googleapis.com/css?family=Open+Sans' rel='stylesheet' type='text/css'>
    <link href='http://fonts.googleapis.com/css?family=Abel' rel='stylesheet' type='text/css'>
    <link href='http://fonts.googleapis.com/css?family=Oswald' rel='stylesheet' type='text/css'>

    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css" />
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css" />

    <link rel="stylesheet" href="//code.jquery.com/ui/1.11.1/themes/smoothness/jquery-ui.css" />
  
    <link rel="stylesheet" href="index.css?<?php echo filectime('index.css') ?>" />    
</head>

<body>

    <div id="header">
        <div id="title">DET - Status Page</div>
    </div>
    
    <div id="navigation">
        <div id="message">Search for request by email or id</div>
    </div>
    
    <div id="main">

    	<div id="search">

    		<div id="search_input"><input type="text"></input></div>
    		<div id="search_button"><button>search</button></div>

    		<div id="search_results" style="display:none;">
	    		<div id="sr_info">Results: <span>x</span> results found for <span>search</span></div>
	    		<div id="sr_table">
	    			<table>
	    				<thead>
	    					<tr><th>id</th><th>email</th><th>status</th><th>details</th></tr>
	    				</thead>
	    				<tbody>
	    				<tbody>
	    			</table>
	    		</div>
	    	</div>

    		<div id="search_popup" style="display:none;">
				Boundary
                <div id="sp_boundary">
                    <div id="sp_bnd_title"></div>
                    <div id="sp_bnd_short"></div>
                    <div id="sp_bnd_link"></div>
                </div>
                <br>
                Datasets
                <div id="sp_datasets"></div>
    		</div>

    	</div>


	    <div id="request" style="display:none;">
	        <div id="top">
	        	status info and search again button
	        </div>

	        <div id="mid">
	        	download data if done
	        </div>

	        <div id="bot">
	        	request details table
	        </div>
	     </div>

    </div>


    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
    <script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.11.0/jquery-ui.min.js"></script>

    <script src="/libs/underscoremin.js"></script>

    <script src="index.js"></script>

</body>

</html>
