$(document).ready(function(){

	var step, request, tmp_request, data_list, map, countryLayer, is_checkedout, total_data_limit;

	total_data_limit = 10;

	var d1_data_limit, project_count, location_count;

	d1_data_limit = 5;
	project_count = -1;
	location_count = -1;

	// prevents multiple checkouts
	is_checkedout = false;

	// current step of process
	step = 0;

	// final output request
	request = {
		"boundary": {},
		"d1_data": {},
		"d2_data": {},
		"email": "",
		"counts": {},
		"total": 0,
		"data_valid": false,
		"checkout_valid": false
	};

	// logs "request" data when returning to previous sections
	// used to determine if any request options have changes
	tmp_request = 0;

	data_list = [];

	// init boundary map
	map_init();

	// get boundary options from mongo
	// build select menu
	// check and active link if given
	process({call:"find_boundaries"}, function (result, status, error){

		if (error) {
			console.log(error);
			return 1
		}

		// console.log(result);
		var bnd_html = '';

	    for (var i=0, ix=_.keys(result).length; i<ix; i++) {
	    	var grp = _.keys(result)[i];
    		bnd_html += '<optgroup label="'+grp+'">';

	    	// sort boundaries by name for each group
	    	result[grp] = _.sortBy(result[grp], 'name')

		    for (var j=0, jx=_.keys(result[grp]).length; j<jx; j++) {
		    	// console.log(result[grp][j]);
		    	var item = result[grp][j];
		    	var path = item['base'] + "/" + item['resources'][0]['path'];

	    		bnd_html += '<option value="' + item['name'] + '" '
	    		bnd_html += 'data-name="' + item['name'] + '" '
	    		bnd_html += 'data-path="'+ path + '" '
	    		bnd_html += 'data-description="'+ item['description'].replace(/\"/g, "'")  + '" '
	    		bnd_html += 'data-title="'+ item['title'].replace(/\"/g, "'")  + '" '
	    		// bnd_html += 'data-source_link="'+ item['source_link'].replace(/\"/g, "'")  +'" '
	    		bnd_html += 'data-group="'+grp+'">' + item['name'] + '</option>';
	    	}
	    	bnd_html += '</optgroup>';
		}

		$('#bnd_options').prop('disabled', false);
		$('#bnd_options').html(bnd_html);
		$('#bnd_options').val(window.location.hash.substr(1)).change();

		if ($("#bnd_options").val() != null) {
			$('#bnd_lock').click();
		}

	});

	// clear checkout page inputs
    $('#co_email input').val("");
	$('#co_terms input').attr('checked', false);

	// load terms of use into textarea on checkout page
	$.ajax({
       url : "termsofuse.txt",
       dataType: "text",
       success : function (data) {
           $("#co_terms textarea").text(data);
       }
    });


	$('#next').click(function () {
		if (step == 0) {
			$('#step').html("Data Selection");
			$('#boundary').hide();
			$('#data').show();
			$('#back button').show();
			$('#next button').hide();
			message("Select data");
			step = 1;

			if (JSON.stringify(request) != JSON.stringify(tmp_request)) {
				get_datasets();
			}

			if (request["data_valid"] == true) {
				$('#next button').show();
			}

		} else if (step == 1) {

			$('#step').html("Checkout");
			$('#data').hide();
			$('#checkout').show();
			$('#next button').hide();
			message("Enter email and review selection before submitting");
			step = 2;
			valid_checkout()

			if (JSON.stringify(request) != JSON.stringify(tmp_request)) {
				build_data_request();
			}

		}
	});

	$('#back').click(function () {
		tmp_request = JSON.parse(JSON.stringify(request));

		if (step == 1) {
			$('#step').html("Boundary Selection");
			$('#data').hide();
			$('#boundary').show();
			map.invalidateSize();
			$('#back button').hide();
			$('#next button').show();
			message("Click the \"Next\" button to continue");
			step = 0;
		} else if (step ==2) {
			$('#step').html("Data Selection");
			$('#checkout').hide();
			$('#data').show();
			$('#next button').show();
			message("Click the \"Next\" button to continue");
			step = 1;
		}
	});


	// boundary select
	$('#bnd_options').on('change', function () {

		// prevents error when loading with no hash link
		if ($(this).val() == null) {
			return;
		}

		var sel = $(this).find('option:selected');
		$('#bnd_title, #data_bnd_title').html(sel.data('title') + " ("+sel.data('group')+" : "+ sel.data('name') +")");
		$('#bnd_description, #data_bnd_description').html(sel.data('description'));
		$('#bnd_link, #data_bnd_link').html(sel.data('source_link'));


        var name = sel.data('name');
        addCountry(name);


		$('#bnd_lock').show();
	});

	// boundary selection lock toggle
	$('#bnd_lock').click(function () {
		var sel = $('#bnd_options').find('option:selected');

		if ($(this).data("locked") == false) {
			$('#bnd_options').prop('disabled', true);
			$(this).html("Deselect");
			$(this).data("locked", true);
			request["boundary"] = sel.data();
			message("Click the \"Next\" button to continue");
			$('#next button').show();

		} else {
			$('#bnd_options').prop('disabled', false);
			$(this).html("Select");
			$(this).data("locked", false);
			request["boundary"] = {};
			message("Select a boundary");
			$('#next button').hide();
		}
	});


	$('#data_tabs>div').on('click', function () {
		$('.data_tab_active').removeClass('data_tab_active');
		$('.data_section_active').removeClass('data_section_active')

		$(this).addClass('data_tab_active');
		$('#'+$(this).data('tab')).addClass('data_section_active');
	})


	// toggle display of datasets
	$('#data').on('click', '.dataset_icon', function () {
		$(this).toggleClass("fa-chevron-down fa-chevron-up");
		$(this).parent().parent().parent().find('.dataset_body').toggle();
	});


	// d1 dataset selection
	$('#d1_datasets').on('change', function () {
		var d1_dataset_name = $(this).val();
		var d1_dataset_data = data_list['d1'][d1_dataset_name];

		$('#d1_info_title').html(d1_dataset_data['title']);
		$('#d1_info_version').html('Version ' + d1_dataset_data['version']);
		$('#d1_info_description').html(d1_dataset_data['description']);

		var d1_sector_html = '<option value="All" title="All" selected>All</option>'
		_.each(d1_dataset_data['sector_list'], function(item) {
			d1_sector_html += '<option value="'+item+'" title="'+item+'">'+item+'</option>';
		})
		$('#d1_sectors').html(d1_sector_html);

		var d1_donor_html = '<option value="All" title="All" selected>All</option>'
		_.each(d1_dataset_data['donor_list'], function(item) {
			d1_donor_html += '<option value="'+item+'" title="'+item+'">'+item+'</option>';
		})
		$('#d1_donors').html(d1_donor_html);

		var d1_year_html = '<option value="All" title="All" selected>All</option>'
		_.each(d1_dataset_data['year_list'], function(item) {
			d1_year_html += '<option value="'+item+'" title="'+item+'">'+item+'</option>';
		})
		$('#d1_years').html(d1_year_html);

		check_matches();

	})

	// d1 filter options change
	$('#d1_bot select').on('change', function () {
		check_matches();
	})

	// d1 add selected filter
	$('#d1_add').on('click', function () {
		var filter_selection;

		filter_selection = get_filter_selection();

		tmp_partial_hash = object_to_hash(filter_selection);

		if (location_count == 0 ) {
			console.log("no locations found matching selection")
		} else if ($('.d1_data').length > d1_data_limit) {
			console.log("you have reached the maximum number of selections ("+d1_data_limit+")")

		} else if ($('#'+tmp_partial_hash).length != 0) {
			console.log("already selected")

		} else {

			time_stamp = Date.now();

			// filter_selection['hash'] = tmp_partial_hash;
			// filter_selection['projects'] = project_count;
			// filter_selection['locations'] = location_count;

			// filter_selection['type'] = "release";

			request['d1_data'][tmp_partial_hash] = filter_selection;

			var selection_html = build_d1_html(filter_selection, project_count, location_count, time_stamp, tmp_partial_hash);
			$('#d1_selected').append(selection_html);

			request["counts"][tmp_partial_hash] = 1;
			sum_counts();

		}
	})

	// d1 remove selected filter
	$('#data_1').on('click', '.d1_remove', function () {
		$parent_selection = $(this).closest('.d1_data');
		var hash = $parent_selection.attr('id');
		delete request['d1_data'][hash];
		delete request["counts"][hash];
		sum_counts();
		$parent_selection.remove();
	})


	// clear all data selections
	$('#d2_clear').click(function () {
		$('#data :checked').each( function () {
			$(this).attr('checked', false);
		});
	});

	// when checkbox changes for a dataset
	// recalculate the number of extracts being request for dataset
	// number extracts = number of extract type * number of files
	// $('#data').on('change', ':checkbox', function () {
	$('#d2_bot').on('change', 'input', function () {
		var parent_data = $(this).closest('.d2_data');
		var parent_name = parent_data.data("name");
		var extract_types_length = parent_data.find('.dataset_options :checked').length;
		var resources_length = parent_data.find('.dataset_temporal :checked').length;

		var total_count = extract_types_length * resources_length;
		request["counts"][parent_name] = total_count;
		sum_counts();
	});


	// monitor email and terms of use
	$('#co_email input, #co_terms input').on('change keyup', function () {
		valid_checkout();
	});

	// on submit button click
	$('#co_submit button').on('click', function () {
		if (request["checkout_valid"] == true && is_checkedout == false) {
			is_checkedout = true;
			request["email"] = $('#co_email input').val();
			request["submit_time"] = Math.floor(Date.now() / 1000);
			request["priority"] = 0;
			request["status"] = -1;
			submit_request();
		}
	});


	// ajax to search.php
	function process(data, callback) {
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

	// update banner message
	function message(str) {
		$('#message').html(str);
	}

	// initialize map
	function map_init() {
		L.mapbox.accessToken = 'pk.eyJ1Ijoic2dvb2RtIiwiYSI6InotZ3EzZFkifQ.s306QpxfiAngAwxzRi2gWg';

		map = L.mapbox.map('map', 'mapbox.streets', {});

		map.setView([15, 0], 2);

		map.dragging.disable();
		map.touchZoom.disable();
		map.doubleClickZoom.disable();
		map.scrollWheelZoom.disable();
	}

	// add geojson to map
	function addCountry(name) {

		var geojsonFeature, error;

		var process_call = 0

		process({call:"get_boundary_geojson", name:name}, function (result, status, e) {
			geojsonFeature = result;
			error = e;
			// console.log(result);

			if (error) {
				console.log(error);
				if (map.hasLayer(countryLayer)) {
					map.removeLayer(countryLayer);
					map.setView([15, 0], 2);
				}
				return 1
			}

			if (map.hasLayer(countryLayer)) {
				map.removeLayer(countryLayer);
			}

			countryLayer = L.geoJson(geojsonFeature, {style: style});
			countryLayer.addTo(map);

			map.fitBounds( countryLayer.getBounds() );
			map.invalidateSize();

		});


		// style polygons
		function style(feature) {
		    return {
		        fillColor: 'red',
		        weight: 1,
		        opacity: 1,
		        color: 'black',
		        fillOpacity: 0.25
		    };
		}
	}


	// get data from boundary tracker
	// build data selection menu
	function get_datasets() {

		request["counts"] = {};

		request['d1_data'] = {};

		$('#data_1 select').empty();
		$('#d1_selected').empty();
		$('#d1_info span').empty();
		$('#d1_matches').hide();
		$('#d2_bot').empty();

		// console.log(request["boundary"]["group"]);

		process({call:"find_relevant_datasets", group:request["boundary"]["group"]}, function (result, status, error){

			if (error) {
				console.log(error);
				return 1
			}

			console.log(result);

			// store data list in external variable for later reference
			data_list = result;

			$('#data_summary_available').html(_.keys(result['d1']).length + _.keys(result['d2']).length);
			$('#data_summary_selected').html(0);

			// d1
			var d1_datasets_html = '<option value="" title="Select a dataset" disabled selected>Select a dataset</option>'
			_.each(_.values(result['d1']), function(dset){
				d1_datasets_html += '<option value='+dset['name']+'>'+dset['title']+' - Version '+dset['version']+'</option>';
			})
			$('#d1_datasets').append(d1_datasets_html)


			// d2
		    _.each(result['d2'], function(dset) {
				var data_html = build_d2_html(dset);
		    	$('#d2_bot').append(data_html);
			})
			// $('#d2_bot').sortable();

			sum_counts();
		});

	}


	// check how many projects match the current aid filter selection
	function check_matches() {
		var filter_selection = get_filter_selection();
		console.log(filter_selection);

		project_count = -1
		location_count = -1;
		process({call:"get_filter_count", filter:filter_selection}, function (result, status, error){

			if (error) {
				console.log(error);
				return 1
			}

			console.log(result);

			var count = result
			$('#d1_matches span').html('<i><span style="font-weight:bold;color:red">'+count['projects']+'</span> Projects with <span style="font-weight:bold;color:red">'+count['locations']+'</span> Locations</i>');
			$('#d1_matches').show();

			project_count = count['projects'];
			location_count = count['locations'];

		});
	}

	// enforce valid aid filters
	// drop additional selections if "All" is selected
	function filter_check(list) {
		return  ! _.isArray(list) || list.length == 0 || list.indexOf("All") > -1 ? ['All'] : list
	}

	function get_filter_selection() {

		var dataset, sectors, donors, years, filter_selection;

		dataset = $('#d1_datasets').val();
		sectors = filter_check($('#d1_sectors').val());
		donors = filter_check($('#d1_donors').val());
		years = filter_check($('#d1_years').val());

		filter_selection = {
			"dataset": dataset,
			"sectors": sectors,
			"donors": donors,
			"years": years,
			"type": "release"
		};

		return filter_selection;
	}

	// uses crypto-js to generate sha1 hash (hex) of json string
	function object_to_hash(input) {
		var ordered, hash, hash_hex;

		ordered = {};
		_.each(_.keys(input).sort(), function (key) {
	  		ordered[key] = input[key];
		});
		console.log(JSON.stringify(ordered))
		hash = CryptoJS.SHA1(JSON.stringify(ordered));
		hash_hex = hash.toString(CryptoJS.enc.Hex)
		console.log(hash_hex)
		return hash_hex;
	}

	// build d1 dataset (aid data, releases) html
	function build_d1_html(filter_selection, project_count, location_count, time_stamp, partial_hash) {
		var data_html = '';

		// open data div
		data_html += '<div class="data d1_data" id="' + partial_hash + '" ' + '" data-type="release">';

			// dataset header
	    	data_html += '<div class="dataset_header ui-icon-minusthick">';

		    	data_html += '<div>'
			    	data_html += '<div class="dataset_h1 dataset_title">' + filter_selection['dataset'] + '</div>';
			    	data_html += '<div class="dataset_h1 dataset_name">(' + partial_hash.substr(0,7)+ '...) '+Date(time_stamp)+'</div>';
			    	data_html += '<button class="d1_remove">Remove</button>';
			    	data_html += '<i class="dataset_icon fa fa-chevron-down fa-2x"></i>';
				data_html += '</div>'

			data_html += '</div>';

			// dataset body
	    	data_html += '<div class="dataset_body">';

		    	data_html += '<div class="dataset_meta">';
			    	data_html += '<div><i><span style="font-weight:bold;color:red">'+project_count+'</span> Projects with <span style="font-weight:bold;color:red">'+location_count+'</span> Locations</i></div><br>';

			    	data_html += '<div class="dataset_h3">Selection Filter</div>';
			    	data_html += '<div class="dataset_h4">';
			    	data_html += '<div class="dataset_meta_info">Sectors:<div>'+filter_selection['sectors']+'</div></div>';
			    	data_html += '<div class="dataset_meta_info">Donors:<div>'+filter_selection['donors']+'</div></div>';
			    	data_html += '<div class="dataset_meta_info">Years:<div>'+filter_selection['years']+'</div></div>';

			    	data_html += '</div>';
		    	data_html += '</div>';

	    	data_html += '</div>';

    	// close data div
    	data_html += '</div>';

		return data_html;
	}


	// build d2 dataset (external raster) html for given dataset object
	function build_d2_html(dataset) {

		var data_html = '';

		// open data div
		data_html += '<div class="data d2_data" id="' + dataset['name'] + '" data-name="' + dataset['name'] + '" data-base="' + dataset['base'] + '" data-type="' + dataset['type'] + '" data-temporal_type="' + dataset['temporal']['type'] + '">';

		// dataset header
    	data_html += '<div class="dataset_header ui-icon-minusthick">';

    	data_html += '<div>'
    	data_html += '<div class="dataset_h1 dataset_title">' + dataset['title'] + '</div>';
    	data_html += '<div class="dataset_h1 dataset_name">(' + dataset['name'] +' - '+ dataset['options']['mini_name'] + ')</div>';
    	data_html += '<i class="dataset_icon fa fa-chevron-down fa-2x"></i>';
		data_html += '</div>'

    	data_html += '<div>'
    	data_html += '<div class="dataset_h2 dataset_type">Type: <span>' + dataset['type'] + '</span></div>';
    	data_html += '<div class="dataset_h2 dataset_range">Range: <span>' + (dataset['temporal']['name'] == "Temporally Invariant" ? dataset['temporal']['name'] : String(dataset['temporal']['start']).substr(0,4) +' - '+ String(dataset['temporal']['end']).substr(0,4)) + '</span></div>';
    	data_html += '<div class="dataset_h2 dataset_step">Step: <span>' + (dataset['temporal']["type"] == "year" ? "yearly" : dataset['temporal']["type"] == "None" ? "N/A" : "Other") + '</span></div>';
    	data_html += '<div class="dataset_h2 dataset_files">Files: <span>' + dataset['resources'].length + '</span></div>';
    	data_html += '<div class="dataset_h2 dataset_toggle"></div>';
    	data_html += '</div>'

		data_html += '</div>';

		// dataset body
    	data_html += '<div class="dataset_body">';


	    	data_html += '<div class="dataset_meta">';
		    	data_html += '<div class="dataset_h3">Meta</div>';
		    	data_html += '<div class="dataset_h4">';
		    	data_html += '<div class="dataset_meta_info">description:<div>'+(dataset['description'] == "" ? "-" : dataset['description'])+'</div></div>';
		    	data_html += '<div class="dataset_meta_info">Variable Description:<div>'+(dataset['options']['variable_description'] == "" ? "-" : dataset['options']['variable_description'])+'</div></div>';

				if (dataset["type"] == "raster") {
		    		data_html += '<div class="dataset_meta_info">Resolution:<div>'+(dataset['options']['resolution'] == "" ? "-" : dataset['options']['resolution'])+' degrees</div></div>';
		    	}
		    	data_html += '</div>';
	    	data_html += '</div>';


		    // temporary raster check since other types might not have options
		    // not really necessary but will serve as placeholder/reminder to
		    // revisit dataset options when we add in other dataset types
	    	if (dataset["type"] == "raster") {

		    	data_html += '<div class="dataset_options">';
			    	data_html += '<div class="dataset_h3">Options</div>';
			    	data_html += '<div class="dataset_h4">';
		    		// data_html += '';
			    	if (dataset["type"] == "raster") {
			    		data_html += '<div class="dataset_opt" data-type="extract_types">Extract Types:';
			    		data_html += '<div>';
		    		   	for (var i=0, ix=dataset['options']['extract_types'].length; i<ix; i++) {
		    		   		// extract type checkbox - select multiple options
		    		   		data_html += '<label><input type="checkbox" value="'+dataset['options']['extract_types'][i]+'">'+dataset['options']['extract_types'][i]+'</label>';

		    		   		// extract type radio - select single option
		    		   		// data_html += '<label><input type="radio" name="'+dataset['name']+'" value="'+dataset['options']['extract_types'][i]+'">'+dataset['options']['extract_types'][i]+'</label>';

		    		   	}
			    		data_html += '</div>';
			    		data_html += '</div>';
			    	}
			    	data_html += '</div>';
		    	data_html += '</div>';
		    }


	    	data_html += '<div class="dataset_temporal">';
		    	data_html += '<div class="dataset_h3">Resources</div>';
		    	data_html += '<div class="dataset_h4">';

		   		for (var i=0, ix=dataset['resources'].length; i<ix; i++) {
					data_html += '<label><input type="checkbox" value="'+dataset['resources'][i]['name']+'" ';
					data_html += 'data-name="'+dataset['resources'][i]["name"]+'" ';
					data_html += 'data-path="'+dataset['resources'][i]["path"]+'" ';
					if (dataset["type"] == "raster") {
						data_html += 'data-reliability="'+dataset['resources'][i]["reliability"]+'" ';
					}
					data_html += '>'+dataset['resources'][i]['name']+'</label>';
				}

		    	data_html += '</div>';
	    	data_html += '</div>';


    	data_html += '</div>';


    	// close data div
    	data_html += '</div>';

    	return data_html;
	}


	// sum counts for all datasets
	function sum_counts() {
		request["total"] = 0;
		if (_.values(request["counts"]).length > 0) {
			for (var i=0, ix=_.values(request["counts"]).length; i<ix; i++) {
				request["total"] += _.values(request["counts"])[i];
			}
		}
		$('#data_summary_selected').html(request["total"]);

		request["data_valid"] = false;
		if (request["total"] > 0 && request["total"] < total_data_limit) {
			$('#next button').show();
			message("Click the \"Next\" button to continue");
			request["data_valid"] = true;
		} else if (request["total"] > 0) {
			message("Too many items selected");
		} else {
			$('#next button').hide();
			message("Select data");

		}
	}


	// build request data objects
	// scans through selected options/resources for all d2 datasets
	function build_data_request() {

		console.log("build data request");

		// request["d1_data"] = {};
		request["d2_data"] = {};

		for (var i=0, ix=_.keys(request["counts"]).length; i<ix; i++) {
			var key = _.keys(request["counts"])[i]
			var $dataset = $('#'+key);
			if (request["counts"][key] > 0) {

				if ($dataset.data("type") == "release") {
					// request["d1_data"][key] = {

					// }

				} else if ($dataset.data("type") == "raster") {
					request["d2_data"][key] = {
						name: key,
						title: $dataset.find('.dataset_title').html(),
						base: $dataset.data("base"),
						type: $dataset.data("type"),
						temporal_type: $dataset.data("temporal_type"),
						options: {},
						files: []
					}

					request["d2_data"][key]["options"]["extract_types"] = [];
					$dataset.find('.dataset_opt').each(function () {
						if ($(this).data("type") == "extract_types") {
							$(this).find(":checked").each(function (){
								request["d2_data"][key]["options"]["extract_types"].push($(this).val());
							})
						}
					})

					$dataset.find('.dataset_temporal :checked').each(function () {
						request["d2_data"][key]["files"].push({name:$(this).data("name"), path:$(this).data("path"), reliability:$(this).data("reliability")});
					})

				}
			}
		}
		console.log(request)

		build_summary();

	}


	// build summary for checkout page
	function build_summary() {
		console.log("build checkout summary");

		var tmp_d1_datasets = [];
		_.each(request['d1_data'], function (x) {
			if (tmp_d1_datasets.indexOf(x['dataset']) == -1) {
				tmp_d1_datasets.push(x['dataset']);
			}
		})

		// summary sentence
		// *** update to include d1 data
		$('#co_s1').html(request["total"]);
		$('#co_s2').html(tmp_d1_datasets.length + _.keys(request["d2_data"]).length);
		$('#co_s3').html(request["boundary"]["title"]);

		// boundary
		var sel = request["boundary"];
		$('#co_bnd_title').html(sel['title'] + " ("+sel['group']+" : "+ sel['name'] +")");
		$('#co_bnd_description').html(sel['description']);
		$('#co_bnd_link').html(sel['source_link']);

		// datasets
		var dset_html = '';

		// d1 data
		for (var i=0, ix=_.keys(request["d1_data"]).length; i<ix; i++) {

			var dset = _.values(request["d1_data"])[i];
			dset_html += '<div class="co_dset">';

		    	dset_html += '<table style="width:100%;"><tbody><tr>'
			    	dset_html += '<td style="width:60%;"><span style="font-weight:bold;">' + dset['dataset'] + '</span> ('+_.keys(request["d1_data"])[i].substr(0,7)+'...) </td>';
			    	dset_html += '<td style="width:20%;">Type: <span>' + dset['type'] + '</span></td>';
			    	dset_html += '<td style="width:20%;">Items: <span>1</span></td>';
		    	dset_html += '</tr>';


		    	dset_html += '<tr><td><b>Sectors: </b>' + dset['sectors'].join(', ');
		    	dset_html += '<tr><td><b>Donors: </b>' + dset['donors'].join(', ');
		    	dset_html += '<tr><td><b>Years: </b>' + dset['years'].join(', ');

		    	dset_html += '</td></tr>';
		    	dset_html += '</tbody></table>';

			dset_html += '</div>';
		}

		// d2 data
		for (var i=0, ix=_.keys(request["d2_data"]).length; i<ix; i++) {
			var dset = _.values(request["d2_data"])[i];
			dset_html += '<div class="co_dset">';

		    	dset_html += '<table style="width:100%;"><tbody><tr>'
			    	dset_html += '<td style="width:60%;"><span style="font-weight:bold;">' + dset['title'] + '</span> ('+dset['name']+') </td>';
			    	dset_html += '<td style="width:20%;">Type: <span>' + dset['type'] + '</span></td>';
			    	dset_html += '<td style="width:20%;">Items: <span>' + (dset['type'] == "raster" ? dset['files'].length * dset['options']['extract_types'].length : dset['files'].length) + '</span></td>';
		    	dset_html += '</tr>';

		    	if (dset['type'] == "raster") {
		    		dset_html += '<tr><td><b>Extract Types Selected: </b>' + dset['options']['extract_types'].join(', ') + '</td></tr>';
		    	}

		    	dset_html += '<tr><td><b>Files: </b>';
		    	for (var j=0, jx=dset['files'].length; j<jx; j++) {
		    		dset_html += j>0 ? ', ' : '';
		    		dset_html += dset['files'][j]['name'];
		    	}

		    	dset_html += '</td></tr>';
		    	dset_html += '</tbody></table>';

			dset_html += '</div>';
		}

		$('#co_datasets').html(dset_html);
	}


	// basic email validation
	// source: http://stackoverflow.com/questions/46155/validate-email-address-in-javascript
	function validate_email(email) {
	    var re = /^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$/i;
	    // var re = /\S+@\S+\.\S+/;
	    return re.test(email);
	}

	function valid_checkout() {
		if (validate_email($('#co_email input').val()) && $('#co_terms input').is(':checked')) {
			request["checkout_valid"] = true;
			$('#co_submit').show();
		} else {
			request["checkout_valid"] = false;
			$('#co_submit').hide();
		}
	}

	// submit request
	function submit_request() {
		console.log("submit request");

		console.log(request);
		// console.log(JSON.stringify(request));

		// start loading animation
		$('#confirmation').html("<p>Please wait a moment while your request is submitted</p>")

		$('#navigation').hide();
		$('#checkout').hide();

		$('#confirmation').show();

		$("html, body").animate({ scrollTop: 0 }, 500);

		var request_id, error;

		// submit request json and run preprocessing script to generate status page
		process({call:"add_request", request:JSON.stringify(request)}, function (result, status, e) {

			console.log(result);

			request_id = result[0];
			error = e;

			chtml = '';

			if (error) {
				console.log(error);

				// display error on confirmation page
				chtml += '<p>There was an error submitting your request. Please try again.</p>';
				chtml += '<p>'+error+'</p>';

			} else {
				// confirm success
				chtml += '<p>Your request has been successfully submitted!</p>';

				// provide request id
				chtml += '<br><p>Request id: ' +request_id+'</p><br>';

				// notify that email has been sent
				chtml += '<p>An email has been sent to '+request['email']+' and an additional email will be sent when your request has been completed.</p>';

				// link to status page with request id
				chtml += '<p>You can check the status of your request and download the results when it has been completed using this link:';
				chtml += '<br><a href="/DET/status/#'+request_id+'">'+window.location.host+'/DET/status/#'+request_id+'</a></p>';
			}

			chtml += '<br><br><p><a href="/DET">Click here to return to the Data Extraction Tool main page.</a></p>';

			$('#confirmation').html(chtml)

			// $('#navigation').hide();
			// $('#checkout').hide();

			// stop loading animation
			//

			// $('#confirmation').show();

		});

	}


})
