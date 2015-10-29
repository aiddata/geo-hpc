$(document).ready(function(){

    // --------------------------------------------------
    // init


	// used to stored mongodb search results
	// indexed by request id
	var search_results = {};

	// possible messages
	var messages = {
		"search": "search for requests by email or request id",
		"select": "select a request from search results",
		"found": "request found",
		"invalid": "invalid email or request id",
		"no_email": "no requests found matching email",
		"no_id": "no request found matching id",
		"search_error": "error searching for requests"
	};

	// status info for requests 
	// status: [display name, associated request field with timestamp]
	var status = {
		"-2":["error", 0],
		"-1":["preprocessing queue", "submit_time"],
		"0":["processing queue", "prep_time"],
		"1":["completed", "complete_time"],
		"2":["preprocessing", "prep_time"],
		"3":["processing", "process_time"]
	};

	// initialize search field
	$('#search_input input').val("");
	
	$("html, body").animate({ scrollTop: 0 }, 500);

	// check hash on page load
    checkSearch(window.location.hash.substr(1), "hash");


    // --------------------------------------------------
    // events


	// check hash on change
    $(window).on('hashchange', function () {
    	var hash = window.location.hash.substr(1);
    	// console.log(hash);
  		checkSearch(hash, "hash");
    });

    // check if search input is valid and trigger search
    $('#search_button button').on('click', function () {
		var search_input = $('#search_input input').val();
		// console.log(search_input);
		checkSearch(search_input, "search");
    });

    // trigger search button when enter key is pressed in search field
	$('#search_input input').keypress(function (e) {
		if (e.which == 13) {
			$('#search_button button').click();
		}
	});

	// scroll to top of page
	$('#return_to_search button').on('click', function () {
		scrollToElement('body');
	});

	// lookup request data using request id and search__results array
	$('#sr_table').on('click', '.request_link', function () {
		var rid = $(this).html();
		var request = search_results[rid];
		add_request_summary(request);
	});


    // --------------------------------------------------
    // functions


	// scroll to specified element
	function scrollToElement(element) {
		$('html, body').animate({
            scrollTop: $(element).offset().top
        }, 500);
	};

	// update banner message
	function message(message_key) {
		$('#message').html(messages[message_key]);
	};

    // checks if input (from hash or search field) is valid and triggers search
    function checkSearch(input, source) {

  		var check_input = validate(input);
  		console.log(check_input);

  		// dump any existing search data from storage
  		search_results = {};

  		// clear request section whenever new query is entered
		clear_request_summary();

		// hide search results while query is being processed
		$('#search_results').hide();

		// if search_val is from hash, set search field value to hash search_val
		// will allow user to adjust search easily if no results are found 
		// or if they return to search page from a request page
		if (source == "hash") {
			$('#search_input input').val(input);
		}


		if (input == "") {
			message("search");

  		} else if (check_input[0]) {
  			// run search on valid query
  			run_search(source, check_input[1], check_input[2]);

  		} else {
			// notify user query was invalid
			message("invalid");
  		}
    };

  	// check hashtag (called on page load or on hashtag change)
  	function validate(value) {

		var search_type = 0;

  		if (validate_email(value)) {
  			search_type = "email";
  		} else if (validate_mongoid(value)) {
  			search_type = "id";
  		}

		return [search_type != 0, search_type, value];
  	};

	// email validation
	// valid name@domain.tld
	// source: http://stackoverflow.com/questions/46155/validate-email-address-in-javascript
	function validate_email(email) {
	    var re = /^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$/i;
	    // var re = /\S+@\S+\.\S+/;
	    return re.test(email);
	};

	// mongoid validation
	// 24 character alphanumeric
	function validate_mongoid(mongoid) {
	    var re = /^[a-z0-9]{24}$/i;
	    return re.test(mongoid);
	};

	// check mongodb (det->queue) for results matching search email or id
	function run_search(source, type, search_val) {

		// look for requests matching search_val
		// call php to search mongo
		var call_data = {call:"requests", search_type:type, search_val:search_val};
		console.log(call_data);
		mongo_search(call_data, function (result, status, error){

			if (error) {
				console.log(error);
				message("search_error");
				return 1;
			}

			console.log(result);

			// store search results in external variable for later reference
			for (var i=0, ix=result.length; i<ix; i++) {
				var tmp_rid = result[i]['_id']['$id'];
				search_results[tmp_rid] = result[i];
			}

			console.log(search_results);
			// check if requests were found
			requests_exist = result.length > 0;

			// update search results sentence with count and query
			$('#count_span').html(result.length);
			$('#query_span').html(search_val);

			// build and update search results
			var sr_html = build_search_results(result);
			console.log(sr_html);
			$('#sr_table tbody').html(sr_html);


			if (requests_exist) {
				// show search results if requests were found
				$('#search_results').show();

				if (source == "hash" && type == "id") {
					message("found");

					// update page with request summary if search is 
					// trigger by hash and query is a request id
					add_request_summary(result[0]);

				} else {
					message("select");
				}

			} else {
				message("no_"+type);
				$('#search_results').hide();
			}

		});
	};

	// ajax to search.php for mongo related calls
	function mongo_search(data, callback) {
		$.ajax ({
	        url: "search.php",
	        data: data,
	        dataType: "json",
	        type: "post",
	        async: true,
	        success: function (result) {
			    callback(result);
			},    
	    	error: function (result, status, error) {
        		callback(result, status, error);
    		}
	    });
	};

	// build table html for search results
	function build_search_results(result) {
		console.log("build_search_results");

		var html = '';

		// for (var i=0, ix=result.length; i<ix; i++) {
		for (var i=result.length-1, ix=0; i>=ix; i--) {

			var r = result[i];

			var status_info = status[String(r['status'])];
			var activity = new Date( r[status_info[1]] * 1000 );

			html += '<tr><td><a class="request_link">'+ r['_id']['$id'] +'</a></td><td>'+ r['email'] +'</td><td>'+ status_info[0] +'</td><td>'+ activity +'</td></tr>';
		}

		return html;
	};

	// clear request section
	function clear_request_summary() {
		$('#request_header').empty();
		$('#request_download').empty();
		$('#request_summary').empty();
		$('#return_to_search').hide();
	};

	// update page when request is selected
	function add_request_summary(request) {
		clear_request_summary();

		var status_info = status[String(request['status'])];
		var activity = new Date( request[status_info[1]] * 1000 );

		// update request header
		var rh_html = '';
		rh_html += '<div>Request id: ' + request['_id']['$id'] + '</div>';
		rh_html += '<div>Email: ' + request['email'] + '</div>'
		rh_html += '<div>Status: (' + request['status'] +') '+ status_info[0] + '</div>'
		rh_html += '<div>Updated: ' + activity + '</div>'

		$('#request_header').html(rh_html);

		var rd_html = '';
		if (request['status'] == 1) {
			// check if request is finished 
			// update request download if it is
			rd_html = '<div><a href="../results/'+request['_id']['$id']+'.zip">Download</a></div>';
		} else {
			rd_html = '<div>Download Not Yet Available</div>';
		}

		$('#request_download').html(rd_html);
		
		// build and update request summary
		var rs_html = build_request_summary(request);
		$('#request_summary').html(rs_html);
		$('#return_to_search').show();

		// scroll page to request section
		scrollToElement('#request');

	};

	// build request summary html
	function build_request_summary(request) {
		console.log("build_request_summary");

		var tmp_d1_datasets = [];
		_.each(request['d1_data'], function (x) {
			if (tmp_d1_datasets.indexOf(x['dataset']) == -1) {
				tmp_d1_datasets.push(x['dataset']);
			}
		})

		var html = '';

		// summary sentence
		html += '<div class="rs_summary">';
			html += '<div class="rs_s1">Total items: ' + request["total"] + '</div>';
			html += '<div class="rs_s2">Datasets: ' + (tmp_d1_datasets.length + _.keys(request["d2_data"]).length) + '</div>';
			html += '<div class="rs_s3">Boundary: ' + request["boundary"]["title"] + '</div>';
		html += '</div><br>';

		// boundary
		var bnd = request["boundary"];
		html += '<div class="rs_boundary">Boundary';
			html += '<div class="rs_bnd_title">' + bnd['title'] + " ("+bnd['group']+" : "+ bnd['name'] +")" +'</div>';
			html += '<div class="rs_bnd_description">' + bnd['description'] + '<div>';
			html += '<div class="rs_bnd_link">' + bnd['source_link'] + '<div>';
		html += '</div><br>';

		// datasets
		html += '<div class"=rs_datasets">Datasets<br>';

		// d1 data
		for (var i=0, ix=_.keys(request["d1_data"]).length; i<ix; i++) {
			var dset = _.values(request["d1_data"])[i];
			html += '<br><div class="rs_dset">';

		    	html += '<table style="width:100%;"><tbody><tr>'
			    	html += '<td style="width:60%;"><span style="font-weight:bold;">' + dset['dataset'] + '</span> ('+_.keys(request["d1_data"])[i].substr(0,7)+'...) </td>';
			    	html += '<td style="width:20%;">Type: <span>' + dset['type'] + '</span></td>';
			    	html += '<td style="width:20%;">Items: <span>1</span></td>';
		    	html += '</tr>';


		    	html += '<tr><td><b>Sectors: </b>' + dset['sectors'].join(', ');
		    	html += '<tr><td><b>Donors: </b>' + dset['donors'].join(', ');
		    	html += '<tr><td><b>Years: </b>' + dset['years'].join(', ');

		    	html += '</td></tr>';
		    	html += '</tbody></table>';

			html += '</div>'; 
		} 

		// d2 data
		for (var i=0, ix=_.keys(request["d2_data"]).length; i<ix; i++) {
			var dset = _.values(request["d2_data"])[i];
			html += '<br><div class="rs_dset">';

		    	html += '<table style="width:100%;"><tbody><tr>'
			    	html += '<td style="width:60%;"><span style="font-weight:bold;">' + dset['title'] + '</span> ('+dset['name']+') </td>';
			    	html += '<td style="width:20%;">Type: <span>' + dset['type'] + '</span></td>';
			    	html += '<td style="width:20%;">Items: <span>' + (dset['type'] == "raster" ? dset['files'].length * dset['options']['extract_types'].length : dset['files'].length) + '</span></td>';
		    	html += '</tr>';

		    	if (dset['type'] == "raster") {
		    		html += '<tr><td><b>Extract Types Selected: </b>' + dset['options']['extract_types'].join(', ') + '</td></tr>';
		    	}

		    	html += '<tr><td><b>Files: </b>';
		    	for (var j=0, jx=dset['files'].length; j<jx; j++) {
		    		html += j>0 ? ', ' : '';
		    		html += dset['files'][j]['name'];
		    	}

		    	html += '</td></tr>';
		    	html += '</tbody></table>';

			html += '</div>'; 
		}

		html += '</div>';

		return html;
	};


})
