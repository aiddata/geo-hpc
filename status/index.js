$(document).ready(function(){

    checkHash();

	// check hashtag on change and on page load
    $(window).on('hashchange', function () {
    	checkHash();
    });
  	
  	// check hashtag (called on page load or on hashtag change)
  	function checkHash(type) {

  		console.log('checkHash: '+ window.location.hash);

      	// do stuff
      	// 
 
  	};



	// basic email validation
	// source: http://stackoverflow.com/questions/46155/validate-email-address-in-javascript
	function validate_email(email) {
	    var re = /^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$/i;
	    // var re = /\S+@\S+\.\S+/;
	    return re.test(email);
	}





	// build request summary html
	function build_request_summary(request) {
		console.log("build_request_summary");

		var html = '';


		// summary sentence
		$('#co_s1').html(request["total"]);
		$('#co_s2').html(_.keys(request["data"]).length);
		$('#co_s3').html(request["boundary"]["title"]);


		html += '<div class="rs_boundary">';

		// boundary
		var bnd = request["boundary"];
		html += '<div class="rs_bnd_title">' + bnd['title'] + " ("+bnd['group']+" : "+ bnd['name'] +")") +'</div>';
		html += '<div class="rs_bnd_short">' + bnd['short']) + '<div>';
		html += '<div class="rs_bnd_link">' + bnd['source_link']) + '<div>';

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
	}






})
