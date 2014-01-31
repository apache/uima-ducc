/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*      http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
/*!
 * ducc.js
 */

var oTable;
var display_table_style;

var ms_reload_min = 5000;

var cluetips_disabled = true;

$(window).resize(function() {
    try {
        var table_style = ducc_preferences_get("table_style");
        if(table_style == "scroll") {
            oTable.fnAdjustColumnSizing();
        }
    }
    catch(err) {
        //ducc_error("$(window).resize",err);
    }   
});

function ducc_cluetips() {
	if(cluetips_disabled) {
		return;
	}
	try {
		$('a.logfileLink').cluetip('destroy');
		$('a.logfileLink').cluetip({
			width: '600px',
	    	attribute: 'href',
	    	activation: 'click',
	    	sticky: true,
	    	closePosition: 'title',
  			closeText: '<img src="opensources/cluetip/cross.png" alt="close" />',
	    	mouseOutClose: true
	  	});
		$('a.classLoad').cluetip('destroy');
		$('a.classLoad').cluetip({
			width: 'auto',
    		local:true,
    		cluetipClass: 'jtip',
    		activation: 'click',
    		sticky: true,
    		titleAttribute: 'title',
    		closePosition: 'title',
    		mouseOutClose: true,
    		dropShadow: false,
   			arrows: true
		});
	}
	catch(err) {
		//ducc_error("ducc_cluetips",err);
	}	
}

function toggleById(id) {
   	$("#"+id).toggle();
}

function ducc_resize() {
    if(navigator.appCodeName == "Mozilla") {
    	// See Jira 3158
    }
    else {
        window.location.href = window.location.href;
    }
}

function ducc_error(loc, err)
{
	var txt;
	txt="There was an error on this page.\n\n";
	txt+="Error location: " + loc + "\n\n";
	txt+="Error description: " + err.message + "\n\n";
	txt+="Click OK to continue.\n\n";
	alert(txt);
}

function ducc_window_close()
{
	try {
  		window.close();
  	}
	catch(err) {
		ducc_error("ducc_window_close",err);
	}	
}

function ducc_identity()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/cluster-name",
			success : function (data) 
			{
				$("#identity").html(data);
				try {
                    a1 = data.split(">");
                    n1 = a1[1];
                    a2 = n1.split("<");
                    n2 = a2[0];
                    name = n2;
                    $(document).attr("title", "ducc-mon: "+name);
                }
                catch(err) {
                    //ducc_error("ducc_identity",err);
                }
			}
		});
	}
	catch(err) {
		ducc_error("ducc_identity",err);
	}
}

function ducc_version()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/version",
			success : function (data) 
			{
				$("#version").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_version",err);
	}
}

function ducc_password_checked()
{
    try {
        $.ajax(
        {
            url : "/ducc-servlet/authenticator-password-checked",
            success : function (data) 
            {
                $("#password_checked_area").html(data);
            }
        });
    }
    catch(err) {
        ducc_error("ducc_password_checked",err);
    }
}

function ducc_authenticator_version()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/authenticator-version",
			success : function (data) 
			{
				$("#authenticator_version_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_authenticator_version",err);
	}
}

function ducc_links()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/login-link",
			success : function (data) 
			{
				$("#login_link_area").html(data);
			}
		});
		$.ajax(
		{
			url : "/ducc-servlet/logout-link",
			success : function (data) 
			{
				$("#logout_link_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_links",err);
	}
}

function ducc_timestamp()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/timestamp",
			success : function (data) 
			{
				$("#timestamp_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_timestamp",err);
	}
}

function ducc_authentication()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/user-authentication-status",
			success : function (data) 
			{
				$("#authentication_area").html(data);
			}
		});
		ducc_links();
	}
	catch(err) {
		ducc_error("ducc_authentication",err);
	}
}

function ducc_utilization()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/cluster-utilization",
			success : function (data) 
			{
				$("#utilization_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_utilization",err);
	}
}

function ducc_init_common()
{
	try {
		data = "...?"
		$("authenticator_version_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_common",err);
	}
}

function ducc_load_common()
{
	try {
		ducc_authenticator_version()
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
	}
	catch(err) {
		ducc_error("ducc_load_common",err);
	}
}

function ducc_load_jobs_head()
{
	ducc_jobs_max_records();
	ducc_jobs_users();
}

var ms_load_jobs_data = +new Date() - ms_reload_min;

function ducc_load_jobs_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_jobs_data + ms_reload_min) {
		return;
	}
	ms_load_jobs_data = ms_now;
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_jobs_data()
	}
	else {
		ducc_load_classic_jobs_data()
	}
}

function ducc_load_classic_jobs_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-jobs-data",
			success : function (data) 
			{
				$("#jobs_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_jobs_data",err);
	}	
}

function ducc_load_scroll_jobs_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-jobs",ducc_load_scroll_jobs_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_jobs_data",err);
	}	
}

function ducc_load_scroll_jobs_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_jobs_callback",err);
	}	
}

function ducc_init_jobs_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#jobs_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_jobs_data",err);
	}	
}

function ducc_load_services_head()
{
	ducc_services_max_records();
	ducc_services_users();
}

var ms_load_services_data = +new Date() - ms_reload_min;

function ducc_load_services_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_services_data + ms_reload_min) {
		return;
	}
	ms_load_services_data = ms_now;
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_services_data()
	}
	else {
		ducc_load_classic_services_data()
	}
}

function ducc_load_classic_services_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-services-data",
			success : function (data) 
			{
				$("#services_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_services_data",err);
	}	
}

function ducc_load_scroll_services_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-services",ducc_load_scroll_services_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_services_data",err);
	}	
}

function ducc_load_scroll_services_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_services_callback",err);
	}	
}

function ducc_init_services_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_services_data",err);
	}
}

function ducc_init_service_summary_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#job_workitems_count_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_service_summary_data",err);
	}
}

function ducc_load_service_summary_data()
{
	try {
		server_url= "/ducc-servlet/service-summary-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#service_summary_area").html(data);
				hide_show();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_service_summary_data",err);
	}	
}

function ducc_init_job_workitems_count_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#job_workitems_count_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_job_workitems_count_data",err);
	}
}

function ducc_load_job_workitems_count_data()
{
	try {
		server_url= "/ducc-servlet/job-workitems-count-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#job_workitems_count_area").html(data);
				hide_show();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_job_workitems_count_data",err);
	}	
}

function ducc_init_job_workitems_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#workitems_data_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_job_workitems_data",err);
	}
}

var ms_load_job_workitems_data = +new Date() - ms_reload_min;

function ducc_load_job_workitems_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_job_workitems_data + ms_reload_min) {
		return;
	}
	ms_load_job_workitems_data = ms_now;
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
		$("#loading_workitems_area").html(data);
		server_url= "/ducc-servlet/job-workitems-data"+location.search;
		$.ajax(
		{
			url : server_url,
			async: true,
			success : function (data) 
			{
				$("#workitems_data_area").html(data);
				hide_show();
				data = "";
				$("#loading_workitems_area").html(data);
			}
		});
	}
	catch(err) {
		data = "";
		$("#loading_workitems_area").html(data);
		ducc_error("ducc_load_job_workitems_data",err);
	}	
}

function ducc_init_job_performance_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#performance_data_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_job_performance_data",err);
	}
}

var ms_load_job_performance_data = +new Date() - ms_reload_min;

function ducc_load_job_performance_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_job_performance_data + ms_reload_min) {
		return;
	}
	ms_load_job_performance_data = ms_now;
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
		$("#loading_performance_area").html(data);
		server_url= "/ducc-servlet/job-performance-data"+location.search;
		$.ajax(
		{
			url : server_url,
			async:true,
			success : function (data) 
			{
				$("#performance_data_area").html(data);
				hide_show();
				data = "";
				$("#loading_performance_area").html(data);
			}
		});
	}
	catch(err) {
		data = "";
		$("#loading_performance_area").html(data);
		ducc_error("ducc_load_job_performance_data",err);
	}
}

function ducc_init_job_specification_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#specification_data_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_job_specification_data",err);
	}
}

var ms_load_job_specification_data = +new Date() - ms_reload_min;

function ducc_load_job_specification_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_job_specification_data + ms_reload_min) {
		return;
	}
	ms_load_job_specification_data = ms_now;
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
		$("#loading_specification_area").html(data);
		server_url= "/ducc-servlet/job-specification-data"+location.search;
		$.ajax(
		{
			url : server_url,
			async: true,
			success : function (data) 
			{
				$("#specification_data_area").html(data);
				hide_show();
				data = "";
				$("#loading_specification_area").html(data);
				sorttable.makeSortable(document.getElementById('specification_table'));
			}
		});
	}
	catch(err) {
		data = "";
		$("#loading_specification_area").html(data);
		ducc_error("ducc_load_job_specification_data",err);
	}	
}

function ducc_init_job_files_data()
{
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    }
    catch(err) {
        ducc_error("ducc_init_job_files_data",err);
    }
}

var ms_load_job_files_data = +new Date() - ms_reload_min;

function ducc_load_job_files_data()
{
    var ms_now = +new Date();
    if(ms_now < ms_load_job_files_data + ms_reload_min) {
        return;
    }
    ms_load_job_files_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        server_url= "/ducc-servlet/job-files-data"+location.search;
        $.ajax(
        {
            url : server_url,
            async: true,
            success : function (data) 
            {
                $("#files_data_area").html(data);
                hide_show();
                data = "";
                $("#loading_files_area").html(data);
                sorttable.makeSortable(document.getElementById('files_table'));
            }
        });
    }
    catch(err) {
        data = "";
        $("#loading_files_area").html(data);
        ducc_error("ducc_load_job_files_data",err);
    }   
}

function ducc_init_reservation_specification_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#specification_data_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_reservation_specification_data",err);
	}
}

var ms_load_reservation_specification_data = +new Date() - ms_reload_min;

function ducc_load_reservation_specification_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_reservation_specification_data + ms_reload_min) {
		return;
	}
	ms_load_reservation_specification_data = ms_now;	
	try {
		server_url= "/ducc-servlet/reservation-specification-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#specification_data_area").html(data);
				hide_show();
				sorttable.makeSortable(document.getElementById('specification_table'));
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_specification_data",err);
	}	
}

function ducc_init_reservation_files_data()
{
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    }
    catch(err) {
        ducc_error("ducc_init_reservation_files_data",err);
    }
}

var ms_load_reservation_files_data = +new Date() - ms_reload_min;

function ducc_load_reservation_files_data()
{
    var ms_now = +new Date();
    if(ms_now < ms_load_reservation_files_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_files_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        server_url= "/ducc-servlet/reservation-files-data"+location.search;
        $.ajax(
        {
            url : server_url,
            async: true,
            success : function (data) 
            {
                $("#files_data_area").html(data);
                hide_show();
                data = "";
                $("#loading_files_area").html(data);
                sorttable.makeSortable(document.getElementById('files_table'));
            }
        });
    }
    catch(err) {
        data = "";
        $("#loading_files_area").html(data);
        ducc_error("ducc_load_reservation_files_data",err);
    }   
}

function ducc_init_service_registry_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#registry_data_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_service_registry_data",err);
	}
}

var ms_load_service_registry_data = +new Date() - ms_reload_min;

function ducc_load_service_registry_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_service_registry_data + ms_reload_min) {
		return;
	}
	ms_load_service_registry_data = ms_now;
	try {
		server_url= "/ducc-servlet/service-registry-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#registry_data_area").html(data);
				hide_show();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_service_registry_data",err);
	}	
}

function ducc_service_update_form_button()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/service-update-get-form-button"+location.search,
			success : function (data) 
			{
				$("#service_update_form_button").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_service_update_form_button",err);
	}
}

var ms_load_service_deployments_data = +new Date() - ms_reload_min;

function ducc_load_service_deployments_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_service_deployments_data + ms_reload_min) {
		return;
	}
	ms_load_service_deployments_data = ms_now;
	try {
		server_url= "/ducc-servlet/service-deployments-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#deployments_list_area").html(data);
				ducc_cluetips();
				hide_show();
     			ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_service_deployments_data",err);
	}
}

function ducc_init_service_deployments_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#deployments_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_service_deployments_data",err);
	}
}

function ducc_init_service_files_data()
{
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    }
    catch(err) {
        ducc_error("ducc_init_service_files_data",err);
    }
}

var ms_load_service_files_data = +new Date() - ms_reload_min;

function ducc_load_service_files_data()
{
    var ms_now = +new Date();
    if(ms_now < ms_load_service_files_data + ms_reload_min) {
        return;
    }
    ms_load_service_files_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        server_url= "/ducc-servlet/service-files-data"+location.search;
        $.ajax(
        {
            url : server_url,
            async: true,
            success : function (data) 
            {
                $("#files_data_area").html(data);
                hide_show();
                data = "";
                $("#loading_files_area").html(data);
                sorttable.makeSortable(document.getElementById('files_table'));
            }
        });
    }
    catch(err) {
        data = "";
        $("#loading_files_area").html(data);
        ducc_error("ducc_load_service_files_data",err);
    }   
}

function ducc_init_service_history_data()
{
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#history_data_area").html(data);
    }
    catch(err) {
        ducc_error("ducc_init_service_history_data",err);
    }
}

var ms_load_service_history_data = +new Date() - ms_reload_min;

function ducc_load_service_history_data()
{
    var ms_now = +new Date();
    if(ms_now < ms_load_service_history_data + ms_reload_min) {
        return;
    }
    ms_load_service_history_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_history_area").html(data);
        server_url= "/ducc-servlet/service-history-data"+location.search;
        $.ajax(
        {
            url : server_url,
            async: true,
            success : function (data) 
            {
                $("#history_data_area").html(data);
                hide_show();
                data = "";
                $("#loading_history_area").html(data);
                sorttable.makeSortable(document.getElementById('history_table'));
            }
        });
    }
    catch(err) {
        data = "";
        $("#loading_history_area").html(data);
        ducc_error("ducc_load_service_history_data",err);
    }   
}

function hide_show() {
	var classpathdata = ducc_appl("classpathdata");
	var c_value = ducc_get_cookie(classpathdata);
	if(c_value == null) {
		c_value = "hide";
	}
	if(c_value == "hide") {
		$('div.showdata').hide();
			$('div.hidedata').show();
	}
	if(c_value == "show") {
		$('div.showdata').show();
			$('div.hidedata').hide();
	}
	$('#showbutton0').click(function(){
		$('div.showdata').show();
		$('div.hidedata').hide();
		ducc_put_cookie(classpathdata,"show")
	});
	$('#hidebutton0').click(function(){
		$('div.showdata').hide();
		$('div.hidedata').show();
		ducc_put_cookie(classpathdata,"hide")
	});
	$('#showbutton1').click(function(){
		$('div.showdata').show();
		$('div.hidedata').hide();
		ducc_put_cookie(classpathdata,"show")
	});
	$('#hidebutton1').click(function(){
		$('div.showdata').hide();
		$('div.hidedata').show();
		ducc_put_cookie(classpathdata,"hide")
	});
}

var ms_load_job_processes_data = +new Date() - ms_reload_min;

function ducc_load_job_processes_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_job_processes_data + ms_reload_min) {
		return;
	}
	ms_load_job_processes_data = ms_now;
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
		$("#loading_processes_area").html(data);
		server_url= "/ducc-servlet/job-processes-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#processes_list_area").html(data);
				ducc_cluetips();
				hide_show();
				data = "";
				$("#loading_processes_area").html(data);
     			ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
	}
	catch(err) {
		data = "";
		$("#loading_processes_area").html(data);
		ducc_error("ducc_load_job_processes_data",err);
	}
}

function ducc_init_job_processes_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#processes_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_job_processes_data",err);
	}
}

var ms_load_reservation_processes_data = +new Date() - ms_reload_min;

function ducc_load_reservation_processes_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_reservation_processes_data + ms_reload_min) {
		return;
	}
	ms_load_reservation_processes_data = ms_now;
	try {
		server_url= "/ducc-servlet/reservation-processes-data"+location.search;
		$.ajax(
		{
			url : server_url,
			success : function (data) 
			{
				$("#processes_list_area").html(data);
				ducc_cluetips();
				hide_show();
     			ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_processes_data",err);
	}
}

function ducc_init_reservation_processes_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#processes_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_reservation_processes_data",err);
	}
}

var ms_load_system_machines_data = +new Date() - ms_reload_min;

function ducc_load_machines_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_system_machines_data + ms_reload_min) {
		return;
	}
	ms_load_system_machines_data = ms_now;
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_machines_data()
	}
	else {
		ducc_load_classic_machines_data()
	}
}

function ducc_load_classic_machines_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-system-machines-data",
			success : function (data) 
			{
				$("#machines_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_machines_data",err);
	}			
}

function ducc_load_scroll_machines_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-machines",ducc_load_scroll_machines_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_machines_data",err);
	}	
}

function ducc_load_scroll_machines_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_machines_callback",err);
	}	
}

function ducc_init_machines_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_machines_data",err);
	}
}

function ducc_reservation_form_button()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-get-form-button",
			success : function (data) 
			{
				$("#reservation_form_button").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_reservation_form_button",err);
	}
}

function ducc_load_reservations_head() 
{
	ducc_reservations_max_records();
	ducc_reservations_users();
}

var ms_load_reservations_data = +new Date() - ms_reload_min;

function ducc_load_reservations_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_reservations_data + ms_reload_min) {
		return;
	}
	ms_load_reservations_data = ms_now;
	ducc_reservation_form_button();
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_reservations_data()
	}
	else {
		ducc_load_classic_reservations_data()
	}
}

function ducc_load_classic_reservations_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-reservations-data",
			success : function (data) 
			{
				$("#reservations_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_reservations_data",err);
	}
}

function ducc_load_scroll_reservations_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-reservations",ducc_load_scroll_reservations_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_reservations_data",err);
	}	
}

function ducc_load_scroll_reservations_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_reservations_callback",err);
	}	
}

function ducc_init_reservations_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#reservations_list_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_reservations_data",err);
	}
}

var ms_load_reservation_scheduling_classes_data = +new Date() - ms_reload_min;

function ducc_load_reservation_scheduling_classes()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_reservation_scheduling_classes_data + ms_reload_min) {
		return;
	}
	ms_load_reservation_scheduling_classes_data = ms_now;
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-scheduling-classes",
			success : function (data) 
			{
				$("#scheduling_class_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_scheduling_classes",err);
	}
}

function ducc_load_reservation_instance_memory_sizes()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-instance-memory-sizes",
			success : function (data) 
			{
				$("#instance_memory_sizes_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_instance_memory_sizes",err);
	}
}

function ducc_load_reservation_instance_memory_units()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-instance-memory-units",
			success : function (data) 
			{
				$("#instance_memory_units_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_instance_memory_units",err);
	}
}

function ducc_load_reservation_number_of_instances()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-number-of-instances",
			success : function (data) 
			{
				$("#number_of_instances_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_number_of_instances",err);
	}		
}

function ducc_load_reservation_submit_button()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/reservation-get-submit-button",
			success : function (data) 
			{
				$("#reservation_submit_button_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_reservation_submit_button",err);
	}			
}

function ducc_load_submit_reservation_data()
{
	try {
		ducc_load_reservation_scheduling_classes();
		ducc_load_reservation_instance_memory_sizes();
		ducc_load_reservation_instance_memory_units();
		ducc_load_reservation_number_of_instances();
		ducc_load_reservation_submit_button();
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
	}
	catch(err) {
		ducc_error("ducc_load_submit_reservation_data",err);
	}			
}

function ducc_init_submit_reservation_data()
{
	try {
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_submit_reservation_data",err);
	}	
}

function ducc_load_job_form()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/job-submit-form",
			success : function (data) 
			{
				$("#job_submit_form_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_job_form",err);
	}		
}

function ducc_load_job_submit_button()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/job-get-submit-button",
			success : function (data) 
			{
				$("#job_submit_button_area").html(data);
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_job_submit_button",err);
	}	
}

function ducc_load_system_administration_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/system-admin-admin-data",
			success : function (data) 
			{
				$("#system_administration_administrators_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
		$.ajax(
		{
			url : "/ducc-servlet/system-admin-control-data",
			success : function (data) 
			{
				$("#system_administration_control_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_system_administration_data",err);
	}
}

function ducc_init_system_administration_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#system_administration_administrators_area").html(data);
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		$("#system_administration_quiesce_area").html(data);
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_system_administration_data",err);
	}
}

function ducc_load_system_classes_data()
{
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_system_classes_data()
	}
	else {
		ducc_load_classic_system_classes_data()
	}
}

function ducc_load_classic_system_classes_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-system-classes-data",
			success : function (data) 
			{
				$("#system_classes_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_system_classes_data",err);
	}	
}

function ducc_load_scroll_system_classes_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-classes",ducc_load_scroll_system_classes_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_system_classes_data",err);
	}	
}

function ducc_load_scroll_system_classes_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_system_classes_callback",err);
	}	
}

function ducc_init_system_classes_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_system_classes_data",err);
	}
}

function ducc_button_show_agents()
{
	try {
		var agents = ducc_appl("agents");
		var c_value = "show";
		ducc_put_cookie(agents,c_value);
		document.getElementById("showbutton").style.display = 'none';
		document.getElementById("hidebutton").style.display = 'block';
	}
	catch(err) {
		ducc_error("ducc_button_show_agents",err);
	}
}

function ducc_show_agents()
{
	try {
		ducc_button_show_agents();
		ducc_refresh("system-daemons");
	}
	catch(err) {
		ducc_error("ducc_show_agents",err);
	}
}

function ducc_button_hide_agents()
{
	try {
		var agents = ducc_appl("agents");
		var c_value = "hide";
		ducc_put_cookie(agents,c_value);
		document.getElementById("showbutton").style.display = 'block';
		document.getElementById("hidebutton").style.display = 'none';
	}
	catch(err) {
		ducc_error("ducc_button_hide_agents",err);
	}
}

function ducc_hide_agents()
{
	try {
		ducc_button_hide_agents();
		ducc_refresh("system-daemons");
	}
	catch(err) {
		ducc_error("ducc_hide_agents",err);
	}
}


function ducc_default_agents()
{
	try {
		var agents = ducc_appl("agents");
		var c_value = ducc_get_cookie(agents);
		if(c_value == "hide") {
			ducc_button_hide_agents();
		}
		else if(c_value == "show") {
			ducc_button_show_agents();
		}
		else {
			ducc_button_hide_agents();
		}
	}
	catch(err) {
		ducc_error("ducc_hide_agents",err);
	}
}

var ms_load_system_daemons_data = +new Date() - ms_reload_min;

function ducc_load_system_daemons_data()
{
	var ms_now = +new Date();
	if(ms_now < ms_load_system_daemons_data + ms_reload_min) {
		return;
	}
	ms_load_system_daemons_data = ms_now;
	var table_style = ducc_preferences_get("table_style");
	if(table_style == "scroll") {
		ducc_load_scroll_system_daemons_data()
	}
	else {
		ducc_load_classic_system_daemons_data()
	}
	ducc_default_agents();
}

function ducc_load_classic_system_daemons_data()
{
	try {
		$.ajax(
		{
			url : "/ducc-servlet/classic-system-daemons-data",
			success : function (data) 
			{
				$("#system_daemons_list_area").html(data);
				ducc_timestamp();
				ducc_authentication();
				ducc_utilization();
				ducc_cluetips();
			}
		});
	}
	catch(err) {
		ducc_error("ducc_load_classic_system_daemons_data",err);
	}	
}

function ducc_load_scroll_system_daemons_data()
{
	try {
		oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-daemons",ducc_load_scroll_system_daemons_callback);
	}
	catch(err) {
		ducc_error("ducc_load_scroll_system_daemons_data",err);
	}	
}

function ducc_load_scroll_system_daemons_callback() 
{
	try {
		ducc_timestamp();
		ducc_authentication();
		ducc_utilization();
		ducc_cluetips();
		oTable.fnAdjustColumnSizing();
	}
	catch(err) {
		ducc_error("ducc_load_scroll_system_daemons_callback",err);
	}	
}

function ducc_init_system_daemons_data()
{
	try {
		data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
		data = "...?"
		$("#timestamp_area").html(data);
		data = "...?"
		$("#authentication_area").html(data);
	}
	catch(err) {
		ducc_error("ducc_init_system_daemons_data",err);
	}
}

function ducc_init(type)
{
	try {
		ducc_identity();
		ducc_version();
		ducc_links();
		ducc_cookies();
		if(type == "jobs") {
			$(document).keypress(function(e) {
  			if(e.which == 13) {
  				ducc_load_jobs_head();
  				ducc_load_jobs_data();
  			}
			});
			ducc_init_jobs_data();
			ducc_load_jobs_head();
			ducc_load_jobs_data();
		}
		if(type == "services") {
			$(document).keypress(function(e) {
  			if(e.which == 13) {
  				ducc_load_services_head();
  				ducc_load_services_data();
  			}
			});
			ducc_init_services_data();
			ducc_load_services_head();
			ducc_load_services_data();
		}
		if(type == "job-details") {
			ducc_init_job_workitems_count_data();
			ducc_init_job_processes_data();
			ducc_init_job_workitems_data();
			ducc_init_job_performance_data();
			ducc_init_job_specification_data();
			ducc_init_job_files_data();
			ducc_load_job_workitems_count_data();
			ducc_load_job_processes_data();
			ducc_load_job_workitems_data();
			ducc_load_job_performance_data();
			ducc_load_job_specification_data();
			ducc_load_job_files_data();
		}
		if(type == "uima-initialization-report") {
		    uima_initialization_report();
		}
		if(type == "reservation-details") {
			ducc_init_reservation_processes_data();
			ducc_init_reservation_specification_data();
			ducc_init_reservation_files_data();
			ducc_load_reservation_processes_data();
			ducc_load_reservation_specification_data();
			ducc_load_reservation_files_data();
		}
		if(type == "service-details") {
			ducc_init_service_summary_data();
			ducc_init_service_deployments_data();
			ducc_init_service_registry_data();
			ducc_init_service_files_data();
			ducc_init_service_history_data();
			ducc_load_service_summary_data();
			ducc_load_service_deployments_data();
			ducc_load_service_registry_data();
			ducc_load_service_files_data();
			ducc_load_service_history_data();
			ducc_service_update_form_button();
		}
		if(type == "system-machines") {
			ducc_init_machines_data();
			ducc_load_machines_data();
		}
		if(type == "reservations") {
			$(document).keypress(function(e) {
  			if(e.which == 13) {
  				ducc_load_reservations_head();
  				ducc_load_reservations_data();
  			}
			});
			ducc_init_reservations_data();
			ducc_load_reservations_head();
			ducc_load_reservations_data();
		}
		if(type == "submit-reservation") {
			ducc_init_submit_reservation_data();
			ducc_load_submit_reservation_data();
		}
		if(type == "system-administration") {
			ducc_init_system_administration_data();
			ducc_load_system_administration_data();
		}
		if(type == "system-classes") {
			ducc_init_system_classes_data();
			ducc_load_system_classes_data();
		}
		if(type == "system-daemons") {
			ducc_init_system_daemons_data();
			ducc_load_system_daemons_data();
		}
		if(type == "authentication-login") {
			ducc_init_common();
			ducc_load_common();
			ducc_password_checked();
			$(document).keypress(function(e) {
  			if(e.which == 13) {
    			ducc_submit_login();
  			}
			});
		}
		if(type == "authentication-logout") {
			ducc_init_common();
			ducc_load_common();
			$(document).keypress(function(e) {
  			if(e.which == 13) {
    			ducc_logout();
  			}
			});
		}
		$.getScript("./js/ducc.local.js", function(){
			ducc_init_local(type);
		});
		var table_style = ducc_preferences_get("table_style");
		display_table_style = table_style;
		ducc_timed_loop(type);
	}
	catch(err) {
		ducc_error("ducc_init",err);
	}
}

function ducc_cookies()
{
	try {
		var refreshmode = ducc_appl("refreshmode");
		var c_value = ducc_get_cookie(refreshmode);
		if(c_value == "automatic") {
			document.duccform.refresh[0].checked = false;
			document.duccform.refresh[1].checked = true;
		}
		else if(c_value == "manual") {
			document.duccform.refresh[0].checked = true;
			document.duccform.refresh[1].checked = false;
		}
		else {
			document.duccform.refresh[0].checked = false;
			document.duccform.refresh[1].checked = true;
			c_value = "automatic";
			ducc_put_cookie(refreshmode, c_value);
		}
	}
	catch(err) {
		ducc_error("ducc_cookies",err);
	}		
}

function uima_initialization_report_summary()
{
    try {
        $.ajax(
        {
            url : "/ducc-servlet/uima-initialization-report-summary"+location.search,
            success : function (data) 
            {
                $("#uima_initialization_report_summary").html(data);
            }
        });
    }
    catch(err) {
        ducc_error("uima_initialization_report_summary",err);location
    }
}

function uima_initialization_report_data()
{
    try {
        $.ajax(
        {
            url : "/ducc-servlet/uima-initialization-report-data"+location.search,
            success : function (data) 
            {
                $("#uima_initialization_report_data").html(data);
            }
        });
    }
    catch(err) {
        ducc_error("uima_initialization_report_data",err);
    }
}

function uima_initialization_report(name)
{
	try {
		uima_initialization_report_summary();
		uima_initialization_report_data();
	}
	catch(err) {
		ducc_error("uima_initialization_report",err);
	}	
}

function ducc_appl(name)
{
	try {
		var appl = "ducc:";
		return appl+name;
	}
	catch(err) {
		ducc_error("ducc_appl",err);
	}	
}

function ducc_jobs_max_records() 
{
	try {
		var d_value = "16";
		var x_value = "1";
		var y_value = "4096";
		var jobsmax = ducc_appl("jobsmax");
		//
		var c_value = ducc_get_cookie(jobsmax);
		var r_value = document.form_selectors.maxrecs_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(jobsmax,c_value);
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
			ducc_put_cookie(jobsmax,c_value);
			document.form_selectors.maxrecs_input.value = c_value;
			$.jGrowl(" max records: "+c_value);
			return;
		}
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_jobs_max_records",err);
	}		
}

function ducc_jobs_users() 
{
	try {
		var jobsusers = ducc_appl("jobsusers");
		var d_value = "";
		var c_value = ducc_get_cookie(jobsusers);
		var r_value = document.form_selectors.users_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(jobsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(r_value == "default") {
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(c_value != r_value) {
			c_value = r_value;
			ducc_put_cookie(jobsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			$.jGrowl(" users: "+c_value);
			return;
		}
		return;
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_jobs_users",err);
	}	
}

function ducc_services_max_records() 
{
	try {
		var d_value = "16";
		var x_value = "1";
		var y_value = "4096";
		var servicesmax = ducc_appl("servicesmax");
		//
		var c_value = ducc_get_cookie(servicesmax);
		var r_value = document.form_selectors.maxrecs_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(servicesmax,c_value);
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
			ducc_put_cookie(servicesmax,c_value);
			document.form_selectors.maxrecs_input.value = c_value;
			$.jGrowl(" max records: "+c_value);
			return;
		}
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_services_max_records",err);
	}		
}

function ducc_services_users() 
{
	try {
		var servicesusers = ducc_appl("servicesusers");
		var d_value = "";
		var c_value = ducc_get_cookie(servicesusers);
		var r_value = document.form_selectors.users_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(servicesusers,c_value);
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(r_value == "default") {
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(c_value != r_value) {
			c_value = r_value;
			ducc_put_cookie(servicesusers,c_value);
			document.form_selectors.users_input.value = c_value;
			$.jGrowl(" users: "+c_value);
			return;
		}
		return;
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_services_users",err);
	}	
}

function ducc_reservations_max_records() 
{
	try {
		var d_value = "16";
		var x_value = "1";
		var y_value = "4096";
		var reservationsmax = ducc_appl("reservationsmax");
		//
		var c_value = ducc_get_cookie(reservationsmax);
		var r_value = document.form_selectors.maxrecs_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(reservationsmax,c_value);
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
			ducc_put_cookie(reservationsmax,c_value);
			document.form_selectors.maxrecs_input.value = c_value;
			$.jGrowl(" max records: "+c_value);
			return;
		}
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_reservations_max_records",err);
	}		
}

function ducc_reservations_users() 
{
	try {
		var reservationsusers = ducc_appl("reservationsusers");
		var d_value = "";
		var c_value = ducc_get_cookie(reservationsusers);
		var r_value = document.form_selectors.users_input.value;
		if(c_value == null) {
			c_value = d_value;
			ducc_put_cookie(reservationsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(r_value == "default") {
			document.form_selectors.users_input.value = c_value;
			return;
		}
		if(c_value != r_value) {
			c_value = r_value;
			ducc_put_cookie(reservationsusers,c_value);
			document.form_selectors.users_input.value = c_value;
			$.jGrowl(" users: "+c_value);
			return;
		}
		return;
	}
	catch(err) {
		throw err;
		//ducc_error("ducc_reservations_users",err);
	}	
}

function ducc_refresh_page(type)
{
	var table_style = ducc_preferences_get("table_style");
	if(display_table_style == table_style) {
		ducc_update_page(type);
	}
	else {
		display_table_style = table_style;
		window.location.reload();
	}
}

function ducc_update_page(type)
{
	try {
		if(type == "jobs") {
			ducc_load_jobs_head();
			ducc_load_jobs_data();
		}
		if(type == "services") {
			ducc_load_services_head();
			ducc_load_services_data();
		}
		if(type == "reservations") {
			ducc_load_reservations_head();
			ducc_load_reservations_data();
		}
		for (var i=0; i < document.duccform.refresh.length; i++) {
			if(type == "jobs") {
				ducc_load_jobs_data();
			}
			if(type == "services") {
				ducc_load_services_data();
			}
			if(type == "reservations") {
				ducc_load_reservations_data();
			}
			if(type == "job-details") {
				ducc_load_job_workitems_count_data();
				ducc_load_job_processes_data();
				ducc_load_job_workitems_data();
				ducc_load_job_performance_data();
				//ducc_load_job_specification_data();
				ducc_load_job_files_data();
			}
			if(type == "reservation-details") {
				//ducc_load_reservation_specification_data();
				ducc_load_reservation_processes_data();
				ducc_load_reservation_files_data();
			}
			if(type == "service-details") {
			    ducc_load_service_history_data();
				ducc_load_service_files_data();
				ducc_load_service_registry_data();
				ducc_load_service_deployments_data();
				ducc_service_update_form_button();
			}
			if(type == "system-machines") {
				ducc_load_machines_data();
			}
			if(type == "system-administration") {
				ducc_load_system_administration_data();
			}
			if(type == "system-daemons") {
				ducc_load_system_daemons_data();
			}
			if(type == "system-classes") {
				ducc_load_system_classes_data();
			}
		}
		$.getScript("./js/ducc.local.js", function(){
			ducc_update_page_local(type);
		});
	}
	catch(err) {
		ducc_error("ducc_update_page",err);
	}			
}

function ducc_refresh_stopped(type) {
	document.getElementById("loading").style.display = 'none';
	document.getElementById("refreshbutton").style.display = 'block';
}

function ducc_refresh_running(type) {
	ducc_refresh_page(type)
	setTimeout(function(){ducc_refresh_stopped(type); type = null},1000);
}

function ducc_refresh_started(type) {
	document.getElementById("refreshbutton").style.display = 'none';
	document.getElementById("loading").style.display = 'block';
	setTimeout(function(){ducc_refresh_running(type); type = null},1);
}

function ducc_refresh(type) {
	setTimeout(function(){ducc_refresh_started(type); type = null},1);
}

function ducc_timed_loop(type) {
	try {
		tid = setTimeout(function(){ducc_timed_loop(type); type = null},30000); // again
		var refreshmode = ducc_appl("refreshmode");
		var c_value = ducc_get_cookie(refreshmode);
		if(c_value == null) {
			c_value = "automatic";
			ducc_put_cookie(refreshmode, c_value);
		}
		if(c_value == "automatic") {
			ducc_refresh(type);
		}
	}
	catch(err) {
		ducc_error("ducc_timed_loop",err);
	}		
}

function ducc_terminate_job(id)
{	
	try {
		$.jGrowl(" Pending termination...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/job-cancel-request"+"?id="+id,
			success : function (data) 
			{
			$.jGrowl(data, { life: 6000 });
			setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_terminate_job",err);
	}	
	return false;
}

function ducc_terminate_service(id)
{	
	try {
		$.jGrowl(" Pending termination...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/service-cancel-request"+"?id="+id,
			success : function (data) 
			{
				$.jGrowl(data, { life: 6000 });
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_terminate_service",err);
	}
	return false;
}

function ducc_service_start(id)
{	
	try {
		$.jGrowl(" Pending start...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/service-start-request"+"?id="+id,
			success : function (data) 
			{
				$.jGrowl(data, { life: 6000 });
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_service_start",err);
	}
	return false;
}

function ducc_service_stop(id)
{	
	try {
		$.jGrowl(" Pending stop...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/service-stop-request"+"?id="+id,
			success : function (data) 
			{
				$.jGrowl(data, { life: 6000 });
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_service_stop",err);
	}
	return false;
}

function ducc_terminate_reservation(id)
{	
	try {
		$.jGrowl(" Pending termination...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/reservation-cancel-request"+"?id="+id,
			success : function (data) 
			{
				$.jGrowl(data, { life: 6000 });
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_terminate_reservation",err);
	}
	return false;
}

function ducc_confirm_accept_jobs()
{
	try {
		var result=confirm("System to accept job submits?");
		if (result==true) {
  			ducc_accept_jobs();
  		}
	}
	catch(err) {
		ducc_error("ducc_confirm_accept_jobs",err);
	}
}

function ducc_accept_jobs(id)
{	
	try {
		$.jGrowl(" Pending jobs submit unblocking...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/jobs-control-request"+"?type=accept",
			success : function (data) 
			{
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_accept_jobs",err);
	}
	return false;
}

function ducc_confirm_block_jobs()
{
	var result=confirm("System to block job submits?");
	if (result==true) {
  		ducc_block_jobs();
  	}
}

function ducc_block_jobs(id)
{	
	try {
		$.jGrowl(" Pending jobs submit blocking...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/jobs-control-request"+"?type=block",
			success : function (data) 
			{
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_block_jobs",err);
	}
	return false;
}

function ducc_release_shares(node, type)
{   
    try {
        $.jGrowl(" Pending release...");
        $.ajax(
        {
            type: 'POST',
            url : "/ducc-servlet/release-shares-request"+"?node="+node+"&"+"type="+type,
            success : function (data) 
            {
            $.jGrowl(data, { life: 6000 });
            setTimeout(function(){window.close();}, 5000);
            }
        });
        setTimeout(function(){window.close();}, 5000);
    }
    catch(err) {
        ducc_error("ducc_release_shares",err);
    }   
    return false;
}

function ducc_confirm_release_shares(node, type)
{
    try {
        var machine = node;
        if(machine == "*") {
            machine = "ALL machines"
        }
        var result=confirm("Release "+type+" shares on "+machine+"?");
        if (result==true) {
            ducc_release_shares(node, type);
        }
    }
    catch(err) {
        ducc_error("ducc_confirm_release_shares",err);
    }
}

function ducc_confirm_terminate_job(id)
{
	try {
		var result=confirm("Terminate job "+id+"?");
		if (result==true) {
  			ducc_terminate_job(id);
  		}
  	}
	catch(err) {
		ducc_error("ducc_confirm_terminate_job",err);
	}
}

function ducc_confirm_terminate_service(id)
{
	try {
		var result=confirm("Terminate service "+id+"?");
		if (result==true) {
  			ducc_terminate_service(id);
  		}
	}
	catch(err) {
		ducc_error("ducc_confirm_terminate_service",err);
	}	
}

function ducc_confirm_service_start(id)
{
	try {
		var result=confirm("Start service "+id+"?");
		if (result==true) {
  			ducc_service_start(id);
  		}
	}
	catch(err) {
		ducc_error("ducc_confirm_service_start",err);
	}	
}

function ducc_confirm_service_stop(id)
{
	try {
		var result=confirm("Stop service "+id+"?");
		if (result==true) {
  			ducc_service_stop(id);
  		}
	}
	catch(err) {
		ducc_error("ducc_confirm_service_stop",err);
	}	
}

function ducc_confirm_terminate_reservation(id)
{
	try {
		var result=confirm("Terminate reservation "+id+"?");
		if (result==true) {
  			ducc_terminate_reservation(id);
  		}
	}
	catch(err) {
		ducc_error("ducc_confirm_terminate_reservation",err);
	}	
}

function ducc_logout()
{
	try {
		$.jGrowl(" Pending logout...");
		$.ajax(
		{
			url : "/ducc-servlet/user-logout",
			success : function (data) 
			{
				setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_logout",err);
	}	
	return false;
}

function ducc_cancel_logout()
{
  	try {
  		window.close();
  	}
	catch(err) {
		ducc_error("ducc_cancel_logout",err);
	}	
}

function ducc_submit_login()
{
	try {
  		var url = document.forms[1].action;
  		var userid = document.forms[1].userid.value
  		var password = document.forms[1].password.value
  		$.jGrowl(" Pending login...");
  		$.ajax({
           type: "POST",
           url: url,
           data: $("#login").serialize(), // serializes the form's elements.
           success: function(data)
           {
               result = data.trim();
               if(result == "success") {
               		//$.jGrowl(" "+result[1]+"="+result[2]);
               		//$.jGrowl(" "+result[3]+"="+result[4]);
               		//ducc_put_cookie(result[1],result[2]);
               		//ducc_put_cookie(result[3],result[4]);
               		$.jGrowl(" "+"login success", { theme: 'jGrowl-success' });
               		setTimeout(function(){window.close();}, 5000);
               }
               else {
               		$.jGrowl(" "+"login failed", { theme: 'jGrowl-error' });
               		$.jGrowl(" "+data, { life: 15000 });
               		setTimeout(function(){window.close();}, 15000);
               }
           }
       	});
  	}
	catch(err) {
		ducc_error("ducc_submit_login",err);
	}		
	return false;
}

function ducc_cancel_login()
{
	try {
  		window.close();
  	}
	catch(err) {
		ducc_error("ducc_cancel_login",err);
	}	
}

function ducc_cancel_submit_reservation()
{
	try {
  		window.close();
  	}
	catch(err) {
		ducc_error("ducc_cancel_submit_reservation",err);
	}	
}

function ducc_submit_reservation()
{
	try {
		var e = document.getElementById("scheduling_class");
		var scheduling_class = e.options[e.selectedIndex].value;
		var e = document.getElementById("instance_memory_size");
		var instance_memory_size = e.options[e.selectedIndex].value;
		var e = document.getElementById("instance_memory_units");
		var instance_memory_units = e.options[e.selectedIndex].value;
		var e = document.getElementById("number_of_instances");
		var number_of_instances = e.options[e.selectedIndex].value;
		var e = document.getElementById("description");
		var description = e.value;
		var e = document.getElementById("wait_for_result_yes");
		var wait_for_result = e.checked;
		if (wait_for_result) {
			document.getElementById("working_area").style.display = 'block';
			document.getElementById("submit_button").disabled = 'disabled';
			
			$.ajax(
			{
				type: 'POST',
				async: false,
				url : "/ducc-servlet/reservation-submit-request",
				//data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'instance_memory_units':instance_memory_units,'number_of_instances':number_of_instances,'description':description},
				data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'number_of_instances':number_of_instances,'description':description},
				success : function (data) 
				{
					$.jGrowl(data, { life: 15000 });
					setTimeout(function(){window.close();}, 15000);
				}
			});
			setTimeout(function(){window.close();}, 15000);
			
			document.getElementById("working_area").style.display = 'none';
		}
		else {
			$.jGrowl(" Pending allocation...");
			$.ajax(
			{
				type: 'POST',
				url : "/ducc-servlet/reservation-submit-request",
				//data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'instance_memory_units':instance_memory_units,'number_of_instances':number_of_instances,'description':description},
				data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'number_of_instances':number_of_instances,'description':description},
				success : function (data) 
				{
					setTimeout(function(){window.close();}, 5000);
				}
			});
			setTimeout(function(){window.close();}, 5000);
		}
	}
	catch(err) {
		ducc_error("ducc_submit_reservation",err);
	}		
	return false;
}

function ducc_update_service(name)
{
	try {
		var e = document.getElementById("autostart");
		var autostart = e.options[e.selectedIndex].value;
		var e = document.getElementById("instances");
		var instances = e.value;
		document.getElementById("update_button").disabled = 'disabled';
		$.ajax(
		{
			type: 'POST',
			async: false,
			url : "/ducc-servlet/service-update-request",
			data: {'id':name,'autostart':autostart,'instances':instances},
			success : function (data) 
			{
				$.jGrowl(data, { life: 15000 });
				setTimeout(function(){window.close();}, 15000);
			}
		});
		setTimeout(function(){window.close();}, 15000);
		document.getElementById("update_button").disabled = '';
	}
	catch(err) {
		ducc_error("ducc_update_service",err);
	}		
	return false;
}

function ducc_put_cookie(name,value)
{
	try {
		var days = 365*31;
		ducc_put_cookie_timed(name,value,days);
	}
	catch(err) {
		ducc_error("ducc_put_cookie",err);
	}	
}

function ducc_put_cookie_timed(name,value,days) 
{
	try {
		if (days) {
			var date = new Date();
			date.setTime(date.getTime()+(days*24*60*60*1000));
			var expires = "; expires="+date.toGMTString();
		}
		else var expires = "";
		document.cookie = name+"="+value+expires+"; path=/";
	}
	catch(err) {
		ducc_error("ducc_put_cookie_timed",err);
	}	
}

function ducc_get_cookie(name) 
{
	var cookie = null;
	try {
		var nameEQ = name + "=";
		var ca = document.cookie.split(';');
		for(var i=0;i < ca.length;i++) {
			var c = ca[i];
			while (c.charAt(0)==' ') c = c.substring(1,c.length);
			if (c.indexOf(nameEQ) == 0) {
				cookie = c.substring(nameEQ.length,c.length);
				break;
			}
		}
	}
	catch(err) {
		ducc_error("ducc_get_cookie",err);
	}	
	return cookie;
}

function ducc_remove_cookie(name) 
{
	try {
		ducc_put_cookie(name,"",-1);
	}
	catch(err) {
		ducc_error("ducc_remove_cookie",err);
	}	
}

function ducc_preferences_reset()
{
	try {
		var key = ducc_appl("table_style");
		var value = "classic";
		//alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		var key = ducc_appl("date_style");
		var value = "long";
		//alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		var key = ducc_appl("description_style");
		var value = "long";
		//alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		var key = ducc_appl("display_style");
        var value = "textual";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key,value);
		var key = ducc_appl("filter_users_style");
		var value = "include";
		//alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		var key = ducc_appl("role");
		var value = "user";
		//alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		//
		ducc_preferences();
	}
	catch(err) {
		ducc_error("ducc_preferences_reset",err);
	}	
}

function ducc_preferences_set(name,value)
{
	try {
		var key = ducc_appl(name);
		//alert("ducc_preferences_set"+" "+"key:"+key+" "+"value:"+value);
		ducc_put_cookie(key,value);
		ducc_preferences();
	}
	catch(err) {
		ducc_error("ducc_preferences_set",err);
	}		
}

function ducc_preferences_get(name)
{
	try {
		var key = ducc_appl(name);
		var value = null;
		value = ducc_get_cookie(key);
		//alert("ducc_preferences_get"+" "+"key:"+key+" "+"value:"+value);
		return value;
	}
	catch(err) {
		ducc_error("ducc_preferences_get",err);
	}
}

function ducc_preferences_table_style() {
	try {
		var key = ducc_appl("table_style");
		var value = ducc_get_cookie(key);
		//alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
		if(value == "classic") {
			document.form_preferences.table_style[0].checked = true;
			document.form_preferences.table_style[1].checked = false;
		}
		else if(value == "scroll") {
			document.form_preferences.table_style[0].checked = false;
			document.form_preferences.table_style[1].checked = true;
		}
		else {
			value = "classic";
			ducc_put_cookie(key, value);
			document.form_preferences.table_style[0].checked = true;
			document.form_preferences.table_style[1].checked = false;
		}
	}
	catch(err) {
		ducc_error("ducc_preferences_table_style",err);
	}
}

function ducc_preferences_date_style() {
	try {
		var key = ducc_appl("date_style");
		var value = ducc_get_cookie(key);
		//alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
		if(value == "long") {
			document.form_preferences.date_style[0].checked = true;
			document.form_preferences.date_style[1].checked = false;
			document.form_preferences.date_style[2].checked = false;
		}
		else if(value == "medium") {
			document.form_preferences.date_style[0].checked = false;
			document.form_preferences.date_style[1].checked = true;
			document.form_preferences.date_style[2].checked = false;
		}
		else if(value == "short") {
			document.form_preferences.date_style[0].checked = false;
			document.form_preferences.date_style[1].checked = false;
			document.form_preferences.date_style[2].checked = true;
		}
		else {
			value = "long";
			ducc_put_cookie(key, value);
			document.form_preferences.date_style[0].checked = true;
			document.form_preferences.date_style[1].checked = false;
			document.form_preferences.date_style[2].checked = false;
		}
	}
	catch(err) {
		ducc_error("ducc_preferences_date_style",err);
	}
}

function ducc_preferences_description_style() {
	try {
		var key = ducc_appl("description_style");
		var value = ducc_get_cookie(key);
		//alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
		if(value == "long") {
			document.form_preferences.description_style[0].checked = true;
			document.form_preferences.description_style[1].checked = false;
		}
		else if(value == "short") {
			document.form_preferences.description_style[0].checked = false;
			document.form_preferences.description_style[1].checked = true;
		}
		else {
			value = "long";
			ducc_put_cookie(key, value);
			document.form_preferences.description_style[0].checked = true;
			document.form_preferences.description_style[1].checked = false;
		}
	}
	catch(err) {
		ducc_error("ducc_preferences_description_style",err);
	}
}

function ducc_preferences_display_style() {
    try {
        var key = ducc_appl("display_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if(value == "textual") {
            document.form_preferences.display_style[0].checked = true;
            document.form_preferences.display_style[1].checked = false;
        }
        else if(value == "visual") {
            document.form_preferences.display_style[0].checked = false;
            document.form_preferences.display_style[1].checked = true;
        }
        else {
            value = "textual";
            ducc_put_cookie(key, value);
            document.form_preferences.display_style[0].checked = true;
            document.form_preferences.display_style[1].checked = false;
        }
    }
    catch(err) {
        ducc_error("ducc_preferences_display_style",err);
    }
}

function ducc_preferences_filter_users_style() {
	try {
		var key = ducc_appl("filter_users_style");
		var value = ducc_get_cookie(key);
		//alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
		if(value == "include") {
			document.form_preferences.filter_users_style[0].checked = true;
			document.form_preferences.filter_users_style[1].checked = false;
			document.form_preferences.filter_users_style[2].checked = false;
			document.form_preferences.filter_users_style[3].checked = false;
		}
		else if(value == "include+active") {
			document.form_preferences.filter_users_style[0].checked = false;
			document.form_preferences.filter_users_style[1].checked = true;
			document.form_preferences.filter_users_style[2].checked = false;
			document.form_preferences.filter_users_style[3].checked = false;
		}
		else if(value == "exclude") {
			document.form_preferences.filter_users_style[0].checked = false;
			document.form_preferences.filter_users_style[1].checked = false;
			document.form_preferences.filter_users_style[2].checked = true;
			document.form_preferences.filter_users_style[3].checked = false;
		}
		else if(value == "exclude+active") {
			document.form_preferences.filter_users_style[0].checked = false;
			document.form_preferences.filter_users_style[1].checked = false;
			document.form_preferences.filter_users_style[2].checked = false;
			document.form_preferences.filter_users_style[3].checked = true;
		}
		else {
			value = "include";
			ducc_put_cookie(key, value);
			document.form_preferences.filter_users_style[0].checked = true;
			document.form_preferences.filter_users_style[1].checked = false;
			document.form_preferences.filter_users_style[2].checked = false;
			document.form_preferences.filter_users_style[3].checked = false;
		}
	}
	catch(err) {
		ducc_error("ducc_preferences_filter_users_style",err);
	}
}

function ducc_preferences_role() {
	try {
		var key = ducc_appl("role");
		var value = ducc_get_cookie(key);
		//alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
		if(value == "user") {
			document.form_preferences.role[0].checked = true;
			document.form_preferences.role[1].checked = false;
		}
		else if(value == "administrator") {
			document.form_preferences.role[0].checked = false;
			document.form_preferences.role[1].checked = true;
		}
		else {
			value = "user";
			ducc_put_cookie(key, value);
			document.form_preferences.role[1].checked = true;
			document.form_preferences.role[0].checked = false;
		}
	}
	catch(err) {
		ducc_error("ducc_preferences_role",err);
	}
}

function ducc_preferences()
{
	try {
		ducc_preferences_table_style();
		ducc_preferences_date_style();
		ducc_preferences_description_style();
		ducc_preferences_display_style();
		ducc_preferences_filter_users_style();
		ducc_preferences_role();
	}
	catch(err) {
		ducc_error("ducc_preferences",err);
	}	
}
