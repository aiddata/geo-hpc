$(document).ready(function(){

	var step, options, tmp_options, data_list, map, countryLayer;

	// current step of process
	step = 0;
	
	// final output options
	options = {
		"boundary": {},
		"data": {}
	};

	// logs "options" data when returning to previous sections
	// used to determine if any options have changes
	tmp_options = 0;

	data_list = [];

	// init boundary map
	map_init();

	// get boundary options from mongo
	// build select menu
	// check and active link if given
	mongo_search({call:"boundaries"}, function (result){
		// console.log(result);
		var bnd_html = '';

	    for (var i=0, ix=_.keys(result).length; i<ix; i++) {
	    	var grp = _.keys(result)[i];
    		bnd_html += '<optgroup label="'+grp+'">';
	    	
		    for (var j=0, jx=_.keys(result[grp]).length; j<jx; j++) {
		    	// console.log(result[grp][j]);
		    	var item = result[grp][j];
		    	var path = item['base'] + "/" + item['resources'][0]['path'];

	    		bnd_html += '<option value="' + item['name'] + '" '
	    		bnd_html += 'data-name="' + item['name'] + '" '
	    		bnd_html += 'data-path="'+ path + '" '
	    		bnd_html += 'data-short="'+ item['short'].replace(/\"/g, "'")  + '" '
	    		bnd_html += 'data-title="'+ item['title'].replace(/\"/g, "'")  + '" '
	    		bnd_html += 'data-source_link="'+ item['source_link'].replace(/\"/g, "'")  +'" '
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


	// boundary select
	$('#bnd_options').on('change', function () {

		if ($(this).val() == null) {
			return;
		}

		var sel = $(this).find('option:selected');
		$('#bnd_title, #data_bnd_title').html(sel.data('title') + " ("+sel.data('group')+" : "+ sel.data('name') +")");
		$('#bnd_short, #data_bnd_short').html(sel.data('short'));
		$('#bnd_link, #data_bnd_link').html(sel.data('source_link'));

		var file = sel.data('path');
		addCountry(file);

		$('#bnd_lock').show();
	});

	// boundary selection lock toggle
	$('#bnd_lock').click(function () {
		var sel = $('#bnd_options').find('option:selected');

		if ($(this).data("locked") == false) {
			$('#bnd_options').prop('disabled', true);
			$(this).html("Deselect");
			$(this).data("locked", true);
			options["boundary"] = sel.data();
			message("Click the \"Next\" button to continue");
			$('#next button').show();

		} else {
			$('#bnd_options').prop('disabled', false);
			$(this).html("Select");
			$(this).data("locked", false);
			options["boundary"] = {};
			message("Select a boundary");
			$('#next button').hide();
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

			if (JSON.stringify(options['boundary']) != JSON.stringify(tmp_options["boundary"])) {
				get_data();
			}
		}
	});

	$('#back').click(function () {
		if (step == 1) {
			$('#step').html("Boundary Selection");
			$('#data').hide();
			$('#boundary').show();
			map.invalidateSize();
			$('#back button').hide();
			$('#next button').show();
			message("Click the \"Next\" button to continue");
			tmp_options = JSON.parse(JSON.stringify(options));
			step = 0;
		}
	});

	$('#data_bot').on('click', '.dataset_icon', function () {
		$(this).toggleClass( "fa-chevron-down fa-chevron-up" );
		$(this).parent().parent().parent().find('.dataset_body').toggle();
	});


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
	function addCountry(file) {

		var geojsonFeature, error;

		process({call:"geojson", file:file}, function (request, status, e) {
			geojsonFeature = request;
			error = e;
			// console.log(request);

			if (error) {
				console.log(error);
				return 1;
			}

			if (map.hasLayer(countryLayer)) {
				map.removeLayer(countryLayer);
			}

			countryLayer = L.geoJson(geojsonFeature, {style: style});
			countryLayer.addTo(map);

			map.fitBounds( countryLayer.getBounds() );
			map.invalidateSize();

		})

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

	// update banner message
	function message(str) {
		$('#message').html(str);
	}

	// get data from boundary tracker
	// build data selection menu
	function get_data() {

		$('#data_bot').empty();

		console.log(options["boundary"]["group"]);

		mongo_search({call:"datasets", group:options["boundary"]["group"]}, function (result){

			console.log(result);

			data_list = result;

			$('#data_summary_available').html(result.length);
			$('#data_summary_selected').html(0);

		    for (var i=0, ix=result.length; i<ix; i++) {

				var data_html = '';

		    	var dataset = result[i];
		    	
		    	console.log(dataset);

	    		data_html += '<div class="data" data-id="' + i + '" data-name="' + dataset['name'] + '">';
		    	
	    		// dataset header
		    	data_html += '<div class="dataset_header ui-icon-minusthick">';

		    	data_html += '<div>'
		    	data_html += '<div class="dataset_h1 dataset_title">' + dataset['title'] + '</div>';
		    	data_html += '<div class="dataset_h1 dataset_name">(' + dataset['name'] + ')</div>';
		    	data_html += '<i class="dataset_icon fa fa-chevron-down fa-2x"></i>';
				data_html += '</div>'
		    	
		    	data_html += '<div>'
		    	data_html += '<div class="dataset_h2 dataset_type">Type: <span>' + dataset['type'] + '</span></div>';
		    	data_html += '<div class="dataset_h2 dataset_range">Range: <span>' + (dataset['temporal']['name'] == "Temporally Invariant" ? dataset['temporal']['name'] : String(dataset['temporal']['start']).substr(0,4) +' - '+ String(dataset['temporal']['end']).substr(0,4)) + '</span></div>';
		    	data_html += '<div class="dataset_h2 dataset_step">Step: <span>' + (dataset['temporal']["type"] == "year" ? "yearly" : dataset['temporal']["type"] == "year month" ? "monthly" : dataset['temporal']["type"] == "None" ? "N/A" : "Other") + '</span></div>';
		    	data_html += '<div class="dataset_h2 dataset_items">Items: <span>' + dataset['resources'].length + '</span></div>';
		    	data_html += '<div class="dataset_h2 dataset_toggle"></div>';
		    	data_html += '</div>'

				data_html += '</div>';

				// dataset body
		    	data_html += '<div class="dataset_body">';

		    	data_html += '<div class="dataset_meta">';
		    	data_html += '</div>';

		    	data_html += '<div class="dataset_options">';
		    	data_html += '</div>';

		    	data_html += '<div class="dataset_temporal">';
		    	data_html += '</div>';

		    	data_html += '</div>';


		    	data_html += '</div>';

		    	$('#data_bot').append(data_html);

			}

			$('#data_bot').sortable();

		});

	}

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
	    	error: function (request, status, error) {
        		callback(request, status, error);
    		}
	    });
	}

	// ajax to process.php for generic calls
	function process(data, callback) {
		$.ajax ({
	        url: "process.php",
	        data: data,
	        dataType: "json",
	        type: "post",
	        async: true,
	        success: function (result) {
			    callback(result);
			},    
	    	error: function (request, status, error) {
        		callback(request, status, error);
    		}
	    });
	}


})
