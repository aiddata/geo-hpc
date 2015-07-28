<!DOCTYPE html>
<html>

<head>
    <meta charset="UTF-8">
    <title>AidData DET 2.0 Alpha</title> 

    <!-- <link href="http://netdna.bootstrapcdn.com/font-awesome/4.0.3/css/font-awesome.css" rel="stylesheet"> -->

    <link href='http://fonts.googleapis.com/css?family=Open+Sans' rel='stylesheet' type='text/css'>
    <link href='http://fonts.googleapis.com/css?family=Abel' rel='stylesheet' type='text/css'>
    <link href='http://fonts.googleapis.com/css?family=Oswald' rel='stylesheet' type='text/css'>

    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css" />
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css" />

    <link rel='stylesheet' href='https://api.tiles.mapbox.com/mapbox.js/v2.1.2/mapbox.css' />
    <link rel="stylesheet" href="//code.jquery.com/ui/1.11.1/themes/smoothness/jquery-ui.css" />
  

    <link rel="stylesheet" href="index.css?<?php echo filectime('index.css') ?>" />    
</head>

<body>

    <div id="header">
        <div id="title">Data Extraction Tool</div>
    </div>
    
    <div id="navigation">
        <ul id="nav_top">
            <li>Boundary</li>
            <li>Raster</li>
            <li>Overview</li>
            <li>Checkout</li>
        </ul>
        <div id="nav_mid">
            <div id="back"><button>Back</button></div>
            <div id="step">Section Title</div>
            <div id="next"><button>Next</button></div>
        </div>
        <div id="nav_bot">
            <div id="message">message</div>
        </div>
    </div>
    
    <div id="main">

        <div id="boundary" class="content">
            <div id="bnd_opts">
                <div>Available Boundaries:</div>
                <select id="bnd_options" size="19">
                </select>
            </div>
            <div id="bnd_map">
                <div id="map"></div>
            </div>
            <div id="bnd_info">
                <div id="bnd_title"></div>
                <div id="bnd_short"></div>
                <div id="bnd_link"></div>

                <button id="bnd_view">View</button>
                <div id="bnd_lock"></div>
            </div>
        </div>

        <div id="raster" class="content" style="display:none;"></div>

        <div id="overview" class="content" style="display:none;"></div>

        <div id="checkout" class="content" style="display:none;"></div>

    </div>


    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
    <script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.11.0/jquery-ui.min.js"></script>

    <script src='https://api.tiles.mapbox.com/mapbox.js/v2.1.2/mapbox.js'></script>

    <!-- // <script src="http://code.highcharts.com/highcharts.js"></script> -->
    <!-- // <script src="http://code.highcharts.com/modules/exporting.js"></script> -->


    <!-- // <script src="/aiddata/libs/leaflet.spin.js"></script> -->
    <!-- // <script src="/aiddata/libs/spin.min.js"></script>     -->
    <script src="/libs/underscoremin.js"></script>
    <!-- // <script src="/aiddata/libs/simple_statistics.js"></script> -->
    <!-- // <script src="/aiddata/libs/URI.js"></script> -->

    <script src="index.js"></script>

</body>

</html>
