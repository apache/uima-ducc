
function ducc_load_experiments_head()
{
	ducc_experiments_max_records();
	ducc_experiments_users();
}

var ms_load_experiments_data = +new Date() - ms_reload_min;

function ducc_load_experiments_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_experiments_data + ms_reload_min) {
		return;
	}
	ms_load_experiments_data = ms_now;
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_experiments_data()
	}
	else {
		ducc_load_classic_experiments_data()
	}
}

function ducc_load_classic_experiments_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/experiments-data",
			success : function (data) 
			{
				$("#experiments_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_experiments_data",err);
	}	
}

function ducc_load_scroll_experiments_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-experiments",ducc_load_scroll_experiments_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_experiments_data",err);
	}	
}

function ducc_load_scroll_experiments_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_experiments_callback",err);
	}	
}

function ducc_init_experiments_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#experiments_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_experiments_data",err);
	}	
}

function ducc_experiments_max_records() 
{
	try {
		var d_value = "16";
		var x_value = "1";
		var y_value = "4096";
		var experimentsmax = ducc_appl("experimentsmax");
		//
		var c_value = ducc_get_cookie(experimentsmax);
		var r_value = document.form_selectors.maxrecs_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(experimentsmax,c_value);
			document.form_selectors.maxrecs_input.value = c_value;
			return;
		}
		if(r_value == "default") {
			document.form_selectors.maxrecs_input.value = c_value;
			//$.jGrowl(" max records: "+c_value);
			return;
		}
		//
		n_value = 1*r_value;
		if(isNaN(n_value)) {
			document.form_selectors.maxrecs_input.value = c_value;
			$.jGrowl(" max records, invalid: "+r_value);
			return;
		}
		r_value = 1*r_value;
		x_value = 1*x_value;
		y_value = 1*y_value;
		if(r_value < x_value) {
			$.jGrowl(" max records, too small: "+r_value+" < "+x_value);
			document.form_selectors.maxrecs_input.value = c_value;
			return;
		}
		if(r_value > y_value) {
			$.jGrowl(" max records, too large: "+r_value+" > "+y_value);
			document.form_selectors.maxrecs_input.value = c_value;
			return;
		}
		//
		if(c_value != r_value) {
			c_value = r_value;
			ducc_put_cookie(experimentsmax,c_value);
			document.form_selectors.maxrecs_input.value = c_value;
			$.jGrowl(" max records: "+c_value);
			return;
		}
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_experiments_max_records",err);
	}		
}

function ducc_experiments_users() 
{
	try {
		var experimentsusers = ducc_appl("experimentsusers");
		var d_value = "";
		var c_value = ducc_get_cookie(experimentsusers);
		var r_value = document.form_selectors.users_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(experimentsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(r_value == "default") {
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(c_value != r_value) {
			c_value = r_value;
			ducc_put_cookie(experimentsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			$.jGrowl(" users: "+c_value);
			return;
		}
		return;
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_experiments_users",err);
	}	
}

var ms_load_experiment_details_data = +new Date() - ms_reload_min;

function ducc_load_experiment_details_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_experiment_details_data + ms_reload_min) {
		return;
	}
	ms_load_experiment_details_data = ms_now;
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
		$("#loading_experiment_details_area").html(data);
		ducc_load_experiment_details_jobs_data();
		server_url= "/ducc-servlet/experiment-details-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#experiment_details_area").html(data);
				ducc_cluetips();
				hide_show();
				data = "";
				$("#loading_experiment_details_area").html(data);
     			ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
	}
	catch(err) {
		data = "";
		$("#loading_experiment_details_area").html(data);
		ducc_error("ducc_load_experiment_details_data",err);
	}
}

function ducc_load_experiment_details_jobs_data()
{
	try {
		server_url= "/ducc-servlet/experiment-details-jobs-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#jobs_list_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_experiments_jobs_data",err);
	}	
}

function ducc_init_experiment_details_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#experiment_details_area").html(data);
		data = "...?"
		$("#jobs_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_experiment_details_data",err);
	}
}

function ducc_init_identify_experiment_details()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#identify_experiment_details_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_identify_experiment_details",err);
	}
}

function ducc_load_identify_experiment_details()
{
	try {
		server_url= "/ducc-servlet/experiment-details-directory"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#identify_experiment_details_area").html(data);
				hide_show();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_identify_experiment_details",err);
	}	
}

function ducc_init_local(type)
{
	try {
		if(type == "experiments") {
			$(document).keypress(function(e) {
  			if(e.which == 13) {
  				ducc_load_experiments_head();
  				ducc_load_experiments_data();
  			}
			});
			ducc_init_experiments_data();
			ducc_load_experiments_head();
			ducc_load_experiments_data();
		}
		if(type == "experiment-details") {
			ducc_init_identify_experiment_details();
			ducc_init_experiment_details_data();
			ducc_load_identify_experiment_details();
			ducc_load_experiment_details_data();
		}
	}
	catch(err) {
		ducc_error("ducc_init_local",err);
	}	
}

function ducc_update_page_local(type)
{
	try {
		if(type == "experiments") {
			ducc_load_experiments_head();
			ducc_load_experiments_data();
			for (var i=0; i < document.duccform.refresh.length; i++) {
				ducc_load_experiments_data();
			}
		}
		if(type == "experiment-details") {
			for (var i=0; i < document.duccform.refresh.length; i++) {
				ducc_load_identify_experiment_details();
				ducc_load_experiment_details_data();
			}
		}
	}
	catch(err) {
		ducc_error("ducc_update_page_local",err);
	}	
}
