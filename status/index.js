$(document).ready(function(){

	// used to stored mongodb search results
	var search_results = []

	// possible messages
	var messages = {
		"search": "search for requests by email or request id",
		"select": "select a request from search results",
		"invalid": "invalid email or request id",
		"no_email": "no requests found matching email",
		"no_id": "no request found matching id",
		"search_error": "error searching for requests"
	};

	// initialize search field
	$('#search_input input').val("");

	// initialize message
	message("search");

	// check hash on page load
    checkSearch(window.location.hash.substr(1), "hash");


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

	// update banner message
	function message(message_key) {
		$('#message').html(messages[message_key]);
	}

    // checks if input (from hash or search field) is valid and triggers search
    function checkSearch(input, source) {
  		var check_input = validate(input);
  		console.log(check_input)

  		if (check_input[0]) {
  			run_search(source, check_input[1], check_input[2])
  		}
    }

  	// check hashtag (called on page load or on hashtag change)
  	function validate(value) {

		var search_type = 0;

  		if (validate_email(value)) {
  			search_type = "email";
  		} else if (validate_mongoid(value)) {
  			search_type = "id";
  		}

		return [search_type != 0, search_type, value]
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
				return 1
			}

			console.log(result);

			// store search results in external variable for later reference
			search_results = result;

			// check if requests were found
			requests_exist = result.length > 0;

			// if search_val is from hash, set search field value to hash search_val
			// will allow user to adjust search easily if no results are found 
			// or if they return to search page from a request page
			if (source == "hash") {
				$('#search_input input').val(search_val);
			}


			if (requests_exist && (source == "search" || type == "email")) {
				// build search results
				build_search_results(result);

			} else if (requests_exist && source == "hash" && type == "id") {
				// build request page
				build_request_page(result);

			} else {
				// notify user no requests were found
				message("no_"+type);
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
	}


	// build table html for search results
	function build_search_results() {

	};

	// build request page html
	function build_request_page() {

	}

	// build request summary html
	function build_request_summary(request) {
		console.log("build_request_summary");

		var html = '';

		// summary sentence
		html += '<div class="rs_summary">';
		html += '<div class="rs_s1">' + request["total"] + '</div>';
		html += '<div class="rs_s2">' + _.keys(request["data"]).length + '</div>';
		html += '<div class="rs_s3">' + request["boundary"]["title"] + '</div>';
		html += '</div>';

		// boundary
		var bnd = request["boundary"];
		html += '<div class="rs_boundary">';
		html += '<div class="rs_bnd_title">' + bnd['title'] + " ("+bnd['group']+" : "+ bnd['name'] +")" +'</div>';
		html += '<div class="rs_bnd_short">' + bnd['short'] + '<div>';
		html += '<div class="rs_bnd_link">' + bnd['source_link'] + '<div>';
		html += '</div';

		// datasets
		html += '<div class"=rs_datasets">';

		for (var i=0, ix=_.keys(request["data"]).length; i<ix; i++) {

			var dset = _.values(request["data"])[i];

			html += '<div class="rs_dset">';

		    	html += '<table style="width:100%;"><tbody><tr>'
			    	html += '<td style="width:60%;"><span style="font-weight:bold;">' + dset['title'] + '</span> ('+dset['name']+') </td>';
			    	html += '<td style="width:20%;">Type: <span>' + dset['type'] + '</span></td>';
			    	html += '<td style="width:20%;">Items: <span>' + (dset['type'] == "raster" ? dset['files'].length * dset['options']['extract_types'].length : dset['files'].length) + '</span></td>';
		    	html += '</tr>';

		    	if (dset['type'] == "raster") {
		    		html += '<tr><td>Extract Types Selected: ' + dset['options']['extract_types'].join(', ') + '</td></tr>';
		    	}

		    	html += '<tr><td>Files: ';
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
