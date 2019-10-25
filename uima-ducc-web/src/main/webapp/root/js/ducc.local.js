
function ducc_load_experiments_head()
{
        ducc_experiments_max_records();
        ducc_experiments_users();
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

// Declare the global variables holding the last-use time
var ms_load_experiment_details_data = +new Date() - ms_reload_min;
var ms_load_experiments_data        = +new Date() - ms_reload_min;

// Load data based on table-style
//
// Construct 3 variables from the type arg: ('experiments' or 'experiment_details')
// - the name of the last-use variable - e.g. ms_load_experiment_details_data
// - the servlet                       - e.g. /ducc-servlet/experiment-details-data
// - the result areas                  - e.g. #experiment_details_area
//
// Drop thw work-in-progress check as the ms_reload_min check should block any simultaneous requests ... how do they occur?

function ducc_load_data(type, params) {
    // Check if too soon after the last use
    var ms_now = +new Date();
    type_ = type.replace(/-/g, '_');
    ms_load = eval('ms_load_' + type_ + '_data');
    if (ms_now < ms_load + ms_reload_min) {
        return;
    }
    eval('ms_load_' + type_ + '_data = ms_now');
    if (params == undefined) {
	params = location.search;
    }
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
	ducc_load_scroll_data(type, params);
    } else {
	ducc_load_classic_data(type, params);
    }
}

function ducc_load_classic_data(type, params) {
    var fname = 'ducc_load_classic_data/' + type;
    var data = null;

    try {
        var servlet = "/ducc-servlet/" + type + "-data" + params
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
	    type_ = type.replace(/-/g, '_');
	    data_area = "#" + type_ + "_area";
            $(data_area).html(data);
            ducc_load_common();
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch(err) {
        ducc_error(fname,err);
    }
}

function ducc_load_scroll_data(type, params)
{
        try {
                oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-" + type + params, ducc_load_scroll_callback);
        }
        catch(err) {
                ducc_error("ducc_load_scroll_data/"+type,err);
        }       
}

function ducc_load_scroll_callback() 
{
        try {
                ducc_load_common();
                oTable.fnAdjustColumnSizing();
        }
        catch(err) {
                ducc_error("ducc_load_scroll_callback",err);
        }       
}

function ducc_init_data(type)
{
        try {
                data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
	        type_ = type.replace(/-/g, '_');
	        data_area = "#" + type_ + "_area";
                $(data_area).html(data);
                data = "...?"
                $("#timestamp_area").html(data);
                data = "...?"
                $("#authentication_area").html(data);
        }
        catch(err) {
                ducc_error("ducc_init_data",err);
        }
}

function ducc_load_identify_experiment_details(params)
{
    if (params == undefined) {
	params = "";
    }
        try {
                server_url= "/ducc-servlet/experiment-details-directory"+location.search+params;
                $.ajax(
                {
                        url : server_url,
                        success : function (data) 
                        {
                                $("#identify_experiment_details_area").html(data);
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
		        // If enter key is pressed refresh the page
                        $(document).keypress(function(e) {
                        if(e.which == 13) {
                                ducc_load_experiments_head();
                                ducc_load_data(type);
                        }
                        });
		        ducc_init_data(type);
                        ducc_load_experiments_head();
		        ducc_load_data(type);
                }
                if(type == "experiment-details") {
                        ducc_init_data(type);
                        ducc_load_identify_experiment_details();
		        ducc_load_data(type);
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
                        ducc_load_data(type);
                }
                if(type == "experiment-details") {
                        ducc_load_identify_experiment_details();
                        ducc_load_data(type);
                }
        }
        catch(err) {
                ducc_error("ducc_update_page_local",err);
        }       
}

function ducc_toggle_task_state(taskid)
{
        try {
                ducc_load_data("experiment-details", location.search+"&taskid="+taskid);
        }
        catch(err) {
                ducc_error("ducc_toggle_task_state",err);
        }       
}

function ducc_restart_experiment()
{
        try {
                ducc_load_identify_experiment_details("&restart=true");
                ducc_load_data("experiment-details");
        }
        catch(err) {
                ducc_error("ducc_restart_experiment",err);
        }       
}

function ducc_terminate_experiment(directory)
{	
	try {
		$.jGrowl(" Pending termination...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/experiment-cancel-request"+"?dir="+directory,
			success : function (data) 
			{
			$.jGrowl(data, { life: 6000 });
			setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_terminate_experiment",err);
	}	
	return false;
}

function ducc_confirm_terminate_experiment(directory)
{
	try {
		var result=confirm("Terminate experiment "+directory+"?");
		if (result==true) {
  			ducc_terminate_experiment(directory);
  		}
  	}
	catch(err) {
		ducc_error("ducc_confirm_terminate_experiment",err);
	}
}
