<!DOCTYPE html>
<html>

<head>
    <meta charset="UTF-8">
    <title>AidData DET 2.0 Alpha</title>

    <link href="http://netdna.bootstrapcdn.com/font-awesome/4.0.3/css/font-awesome.css" rel="stylesheet">

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

        <div id="navigation">
            <ul id="nav_top">
                <li>Boundary</li>
                <li>Data</li>
                <li>Checkout</li>
            </ul>
            <div id="nav_mid">
                <div id="back"><button style="display:none;">Back</button></div>
                <div id="step">Boundary Selection</div>
                <div id="next"><button style="display:none;">Next</button></div>
            </div>
            <div id="nav_bot">
                <div id="message">Select a boundary to get started</div>
            </div>
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
                <div id="bnd_description"></div>
                <div id="bnd_link"></div>
                <button id="bnd_lock" style="display:none;" data-locked="false">Select</button>
            </div>
        </div>

        <div id="data" class="content" style="display:none;">
            <div id="data_info">
                <div id="data_bnd">
                    <span><b>Boundary Info</b></span>
                    <div>
                        <div id="data_bnd_title"></div>
                        <div id="data_bnd_description"></div>
                        <div id="data_bnd_link"></div>
                    </div>
                </div>
                <div id="data_summary">
                    <span><b>Data Summary</b></span>
                    <div>
                        <div>Datasets Available: <span id="data_summary_available">#</span></div>
                        <div>Items Selected: <span id="data_summary_selected">#</span></div>
                    </div>
                </div>
            </div>

            <div id="data_tabs">
                <div data-tab="data_1" class="data_tab_active">Aid Data</div>
                <div data-tab="data_2">External Data</div>
                <!-- <div data-tab="data_3">Additional Data</div> -->
            </div>


            <div id="data_1" class="data_section data_section_active">
                <div id="d1_filter">

                    <div id="d1_top">
                        <div>
                            Available Datasets:
                            <select id="d1_datasets"></select>
                        </div>

                        <div id="d1_top_right">
                            <div id="d1_matches">
                                <i>Matches: </i>
                                <span></span>
                            </div>

                            <button id="d1_add">Add Selection</button>

                        </div>
                    </div>

                    <div id="d1_bot">
                        <div id="d1_c1">
                            Sectors<br>
                            <select id="d1_sectors" multiple size=10></select>
                        </div>

                        <div id="d1_c2">
                            Donors<br>
                            <select id="d1_donors" multiple size=10></select>
                        </div>

                        <div id="d1_c3">
                            Years<br>
                            <select id="d1_years" multiple size=10></select>
                        </div>

                        <div id="d1_c4">
                            <br>
                            <div id="d1_info">

                                <b><span id="d1_info_title"></span></b>
                                <br>
                                <i><span id="d1_info_version"></span></i>

                                <br><br>
                                <span id="d1_info_description"></span>

                            </div>
                        </div>

                    </div>

                </div>

                Currently Selected:
                <div id="d1_selected">

                </div>
            </div>


            <div id="data_2" class="data_section">
                <div id="d2_top">
                    <span>Available Data:</span>
                    <button id="d2_clear">Clear All</span>
                </div>
                <div id="d2_bot"></div>
            </div>

            <div id="data_3" class="data_section">
                Coming soon...
            </div>

        </div>

        <div id="checkout" class="content" style="display:none;">
            <div id="co_top">
                <div id="co_summary">Request includes <span id="co_s1">#</span> extractions across <span id="co_s2">#</span> dataset(s) for boundary "<span id="co_s3">X</span>"</div>
                <div id="co_email">Email: <input type="text" placeholder="enter email"></input></div>
                <div id="co_terms">
                    <div><textarea rows="4" cols="50">
                        **
                        - Terms of Use
                        **
                    </textarea></div>
                    <div><label>I agree to the Terms of Use: <input type="checkbox" value="false"></label></div>
                </div>
                <div id="co_submit" style="display:none;">
                    <div>Submit Request?</div>
                    <button>Submit</button>
                </div>
            </div>
            <div id="co_bot">
                Boundary
                <div id="co_boundary">
                    <div id="co_bnd_title"></div>
                    <div id="co_bnd_description"></div>
                    <div id="co_bnd_link"></div>
                </div>
                <br>
                Datasets
                <div id="co_datasets"></div>
            </div>
        </div>

        <div id="confirmation" class="content" style="display:none;"></div>

    </div>


    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
    <script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.11.0/jquery-ui.min.js"></script>

    <script src='https://api.tiles.mapbox.com/mapbox.js/v2.1.2/mapbox.js'></script>

    <script src="libs/underscoremin.js"></script>
    <script src="libs/crypto-js-sha1.js"></script>

    <script src="index.js"></script>

</body>

</html>
