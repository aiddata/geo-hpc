$(document).ready(function(){

	var map, countryLayer

	L.mapbox.accessToken = 'pk.eyJ1Ijoic2dvb2RtIiwiYSI6InotZ3EzZFkifQ.s306QpxfiAngAwxzRi2gWg';

	map = L.mapbox.map('map', 'mapbox.streets', {
	  zoomControl: false
	})

	map.setView([15, 0], 2);

	map.dragging.disable();
	map.touchZoom.disable();
	map.doubleClickZoom.disable();
	map.scrollWheelZoom.disable();


	$('#bnd_options').on('change', function () {
		// console.log($(this).data())
		var sel = $(this).find('option:selected');
		$('#bnd_title').html(sel.data('title') + " ("+sel.data('group')+" : "+ sel.val() +")")
		$('#bnd_short').html(sel.data('short'))
		$('#bnd_link').html(sel.data('source_link'))

	})

	$('#bnd_view').click(function () {
		var sel = $('#bnd_options').find('option:selected');
		var file = sel.data('path');
		addCountry(file)

	})

	mongo_search({call:"boundaries"}, function (result){
		// console.log(result);

		var bnd_html = ''
	    
	    for (var i=0, ix=_.keys(result).length; i<ix; i++) {
	    	var grp = _.keys(result)[i]
    		bnd_html += '<optgroup label="'+grp+'"></optgroup>'
	    	
		    for (var j=0, jx=_.keys(result[grp]).length; j<jx; j++) {
		    	// console.log(result[grp][j])
		    	var item = result[grp][j]

		    	var path = item['base'] + "/" + item['resources'][0]['path']
	    		bnd_html += '<option value="' + item['name'] + '" data-path="'+ path +'" data-short="'+ item['short'].replace(/\"/g, "'")  +'" data-title="'+ item['title'].replace(/\"/g, "'")  +'" data-source_link="'+ item['source_link'].replace(/\"/g, "'")  +'" data-group="'+grp+'">' + item['name'] + '</option>'
	    	}
		}

		$('#bnd_options').html(bnd_html)
	})


	// generic ajax call to search.php
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
	    })
	}


	// generic ajax call to process.php
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
	    })
	}


	function addCountry(file) {

		var geojsonFeature, error

		process({call:"geojson", file:file}, function (request, status, e) {
			geojsonFeature = request;
			error = e;
			// console.log(request)

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
			
		})

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


})
