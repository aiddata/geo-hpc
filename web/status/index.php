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
    
	    <div id="navigation">
	        <div id="message"></div>
	    </div>
    </div>
    
    <div id="main">

    	<div id="search">

    		<div id="search_input"><input type="text"></input></div>
    		<div id="search_button"><button>search</button></div>

    		<div id="search_results" style="display:none;">
	    		<div id="sr_info">Results: <span id="count_span">x</span> request(s) found matching <span id="query_span">search</span></div>
	    		<div id="sr_table">
	    			<table>
	    				<thead>
	    					<tr><th>id</th><th>email</th><th>status</th><th>updated</th></tr>
	    				</thead>
	    				<tbody>
	    					<!-- search results -->
	    				</tbody>
	    			</table>
	    		</div>
	    	</div>
    	</div>


	    <div id="request">

	    	<div id="return_to_search"><button>Search again</button></div>

	        <div id="request_header">
	        	<!-- request id, email, status info -->
	        </div>

	        <div id="request_download">
	        	<!-- download data if done -->
	        </div>

	        <div id="request_summary">
	        	<!-- request details table -->
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
