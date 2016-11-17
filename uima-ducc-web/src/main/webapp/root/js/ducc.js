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

var ms_timeout = 25000;

var cluetips_disabled = true;

$(window).resize(function() {
    try {
        var table_style = ducc_preferences_get("table_style");
        if (table_style == "scroll") {
            oTable.fnAdjustColumnSizing();
        }
    } catch (err) {
        //ducc_error("$(window).resize",err);
    }
});

function ducc_console_warn(text) {
    var date = new Date();
    console.warn(date.toLocaleString() + " " + text);
}

var flag_debug = false;

function ducc_console_debug(text) {
    if(flag_debug) {
        var date = new Date();
        console.log(date.toLocaleString() + " " + text);
    }
}

function ducc_console_enter(fname) {
    var text = fname + " " + "enter";
    ducc_console_debug(text);
}

function ducc_console_exit(fname) {
    var text = fname + " " + "exit";
    ducc_console_debug(text);
}

function ducc_console_success(fname) {
    var text = fname + " " + "success";
    ducc_console_debug(text);
}

function ducc_console_fail(fname, textStatus) {
    var text = fname + " " + "fail:" + " " + textStatus;
    ducc_console_warn(text);
}

function ducc_cluetips() {
    if (cluetips_disabled) {
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
            local: true,
            cluetipClass: 'jtip',
            activation: 'click',
            sticky: true,
            titleAttribute: 'title',
            closePosition: 'title',
            mouseOutClose: true,
            dropShadow: false,
            arrows: true
        });
    } catch (err) {
        //ducc_error("ducc_cluetips",err);
    }
}

function toggleById(id) {
    $("#" + id).toggle();
}

function ducc_resize() {
    if (navigator.appCodeName == "Mozilla") {
        // See Jira 3158
    } else {
        window.location.href = window.location.href;
    }
}

function ducc_error(loc, err) {
    var fname = "ducc_error";
    var txt;
    txt = "There was an error on this page.\n\n";
    txt += "Error location: " + loc + "\n\n";
    txt += "Error description: " + err.message + "\n\n";
    txt += "Click OK to continue.\n\n";
    alert(txt);
}

function ducc_window_close() {
    var fname = "ducc_window_close";
    try {
        window.close();
    } catch (err) {
        ducc_error(fname, err);
    }
}

var wip_identity = false;

function ducc_identity() {
    var fname = "ducc_identity";
    var data = null;
    if(wip_identity) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_identity = true;
    try {
        var servlet = "/ducc-servlet/cluster-name";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_identity = false;
            $("#identity").html(data);
            try {
                var a1 = data.split(">");
                var n1 = a1[1];
                var a2 = n1.split("<");
                var n2 = a2[0];
                var name = n2;
                $(document).attr("title", "ducc-mon: " + name);
                data = null;
                ducc_console_success(fname);
            } catch (err) {
                var message = fname + ".error: " + err;
                ducc_console_warn(message);
            }
        }).fail(function(jqXHR, textStatus) {
            wip_identity = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_identity = false;
        ducc_error(fname, err);
    }
}

var wip_version = false;

function ducc_version() {
    var fname = "ducc_version";
    var data = null;
    if(wip_version) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_version = true;
    try {
        var servlet = "/ducc-servlet/version";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_version = false;
            $("#version").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_version = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_version = false;
        ducc_error(fname, err);
    }
}

var wip_home = false;

function ducc_home() {
    var fname = "ducc_home";
    var data = null;
    if(wip_home) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_home = true;
    try {
        var servlet = "/ducc-servlet/home";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_home = false;
            $("#home").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_home = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_home = false;
        ducc_error(fname, err);
    }
}

var wip_password_checked = false;

function ducc_password_checked() {
    var fname = "ducc_password_checked";
    var data = null;
    if(wip_password_checked) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_password_checked = true;
    try {
        var servlet = "/ducc-servlet/authenticator-password-checked";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_password_checked = false;
            $("#password_checked_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_password_checked = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_password_checked = false;
        ducc_error(fname, err);
    }
}

var wip_authenticator_version = false;

function ducc_authenticator_version() {
    var fname = "ducc_authenticator_version";
    var data = null;
    if(wip_authenticator_version) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_authenticator_version = true;
    try {
        var servlet = "/ducc-servlet/authenticator-version";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_authenticator_version = false;
            $("#authenticator_version_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_authenticator_version = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_authenticator_version = false;
        ducc_error(fname, err);
    }
}

var wip_link_login = false;

function ducc_link_login() {
    var fname = "ducc_link_login";
    var data = null;
    if(wip_link_login) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_link_login = true;
    try {
        var servlet = "/ducc-servlet/login-link";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_link_login = false;
            $("#login_link_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_link_login = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_link_login = false;
        ducc_error(fname, err);
    }
}

var wip_link_logout = false;

function ducc_link_logout() {
    var fname = "ducc_link_logout";
    var data = null;
    if(wip_link_logout) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_link_logout = true;
    try {
        var servlet = "/ducc-servlet/logout-link";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_link_logout = false;
            $("#logout_link_area").html(data);
           data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_link_logout = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_link_logout = false;
        ducc_error(fname, err);
    }
}

var ms_links = +new Date() - ms_reload_min;

function ducc_links() {
    var fname = "ducc_links";
    var ms_now = +new Date();
    if (ms_now < ms_links + ms_reload_min) {
        return;
    }
    ms_links = ms_now;
    ducc_link_login();
    ducc_link_logout();
}

var wip_timestamp = false;

function ducc_timestamp() {
    var fname = "ducc_timestamp";
    var data = null;
    if(wip_timestamp) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_timestamp = true;
    try {
        var servlet = "/ducc-servlet/timestamp";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_timestamp = false;
            $("#timestamp_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_timestamp = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_timestamp = false;
        ducc_error(fname, err);
    }
}

var wip_authentication = false;

function ducc_authentication() {
    var fname = "ducc_authentication";
    var data = null;
    if(wip_authentication) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_authentication = true;
    try {
        var servlet = "/ducc-servlet/user-authentication-status";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_authentication = false;
            $("#authentication_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_authentication = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_authentication = false;
        ducc_error(fname, err);
    }
}

var wip_utilization = false;

function ducc_utilization() {
    var fname = "ducc_utilization";
    var data = null;
    if(wip_utilization) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_utilization = true;
    try {
        var servlet = "/ducc-servlet/cluster-utilization";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_utilization = false;
            $("#utilization_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_utilization = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_utilization = false;
        ducc_error(fname, err);
    }
}

function ducc_init_common() {
    var fname = "ducc_init_common";
    var data = null;
    try {
        data = "...?"
        $("authenticator_version_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_common = +new Date() - ms_reload_min;

function ducc_load_common() {
    var fname = "ducc_load_common";
    var ms_now = +new Date();
    if (ms_now < ms_load_common + ms_reload_min) {
        return;
    }
    ms_load_common = ms_now;
    try {
        ducc_authenticator_version()
        ducc_timestamp();
        ducc_authentication();
        ducc_utilization();
        ducc_links();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_viz_data() {
    var fname = "ducc_init_viz_data";
    var data = null;
    try {
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_viz_head() {
    var fname = "ducc_load_viz_head";
    try {

    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_viz_data = +new Date() - ms_reload_min;
var wip_viz = false;

function ducc_load_viz_data() {
    var fname = "ducc_load_viz_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_viz_data + ms_reload_min) {
        return;
    }
    ms_load_viz_data = ms_now;
    if(wip_viz) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_viz = true;
    try {
        var servlet = "/ducc-servlet/viz-nodes";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_viz = false;
            $("#viz-nodes").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            wip_viz = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_viz = false;
        ducc_error(fname, err);
    }
}

function ducc_load_jobs_head() {
    var fname = "ducc_load_jobs_head";
    ducc_jobs_max_records();
    ducc_jobs_users();
}

var ms_load_jobs_data = +new Date() - ms_reload_min;

function ducc_load_jobs_data() {
    var fname = "ducc_load_jobs_data";
    var ms_now = +new Date();
    if (ms_now < ms_load_jobs_data + ms_reload_min) {
        return;
    }
    ms_load_jobs_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_jobs_data()
    } else {
        ducc_load_classic_jobs_data()
    }
}

var wip_jobs = false;

function ducc_load_classic_jobs_data() {
    var fname = "ducc_load_classic_jobs_data";
    var data = null;
    if(wip_jobs) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_jobs = true;
    try {
        var servlet = "/ducc-servlet/classic-jobs-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_jobs = false;
            $("#jobs_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_jobs = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_jobs = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_jobs_data() {
    var fname = "ducc_load_scroll_jobs_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-jobs", ducc_load_scroll_jobs_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_jobs_callback() {
    var fname = "ducc_load_scroll_jobs_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_jobs_data() {
    var fname = "ducc_init_jobs_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#jobs_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_services_head() {
    var fname = "ducc_load_services_head";
    ducc_services_max_records();
    ducc_services_users();
    ducc_load_services_records_ceiling();
}

var ms_load_services_data = +new Date() - ms_reload_min;

function ducc_load_services_data() {
    var fname = "ducc_load_services_data";
    var ms_now = +new Date();
    if (ms_now < ms_load_services_data + ms_reload_min) {
        return;
    }
    ms_load_services_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_services_data()
    } else {
        ducc_load_classic_services_data()
    }
}

var wip_services_records_ceiling = false;

function ducc_load_services_records_ceiling() {
    var fname = "ducc_load_services_records_ceiling";
    var data = null;
    if(wip_services_records_ceiling) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_services_records_ceiling = true;
    try {
        var servlet = "/ducc-servlet/services-records-ceiling";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_services_records_ceiling = false;
            $("#services_records_ceiling_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
        	wip_services_records_ceiling = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
    	wip_services_records_ceiling = false;
        ducc_error(fname, err);
    }
}

var wip_services = false;

function ducc_load_classic_services_data() {
    var fname = "ducc_load_classic_services_data";
    var data = null;
    if(wip_services) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_services = true;
    try {
        var servlet = "/ducc-servlet/classic-services-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_services = false;
            $("#services_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_services = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_services = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_services_data() {
    var fname = "ducc_load_scroll_services_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-services", ducc_load_scroll_services_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_services_callback() {
    var fname = "ducc_load_scroll_services_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_services_data() {
    var fname = "ducc_init_services_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_service_summary_data() {
    var fname = "ducc_init_service_summary_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#service_summary_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_service_summary_data = +new Date() - ms_reload_min;
var wip_service_summary = false;

function ducc_load_service_summary_data() {
    var fname = "ducc_load_service_summary_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_summary_data + ms_reload_min) {
        return;
    }
    ms_load_service_summary_data = ms_now;
    if(wip_service_summary) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_summary = true;
    try {
        var servlet = "/ducc-servlet/service-summary-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_summary = false;
            $("#service_summary_area").html(data);
            hide_show();
           data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_service_summary = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_service_summary = false;
        ducc_error(fname, err);
    }
}

function ducc_init_broker_summary_data() {
    var fname = "ducc_init_broker_summary_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#broker_summary_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_broker_summary_data = +new Date() - ms_reload_min;
var wip_broker_summary = false;

function ducc_load_broker_summary_data() {
    var fname = "ducc_load_broker_summary_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_broker_summary_data + ms_reload_min) {
        return;
    }
    ms_load_broker_summary_data = ms_now;
    if(wip_broker_summary) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_broker_summary = true;
    try {
        var servlet = "/ducc-servlet/broker-summary-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_broker_summary = false;
            $("#broker_summary_area").html(data);
            hide_show();
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_broker_summary = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_broker_summary = false;
        ducc_error(fname, err);
    }
}

function ducc_init_job_workitems_count_data() {
    var fname = "ducc_init_job_workitems_count_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#job_workitems_count_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_workitems_count_data = +new Date() - ms_reload_min;
var wip_job_workitems_count_data = false;

function ducc_load_job_workitems_count_data() {
    var fname = "ducc_load_job_workitems_count_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_workitems_count_data + ms_reload_min) {
        return;
    }
    ms_load_job_workitems_count_data = ms_now;
    if(wip_job_workitems_count_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_workitems_count_data = true;
    try {
        var servlet = "/ducc-servlet/job-workitems-count-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_workitems_count_data = false;
            $("#job_workitems_count_area").html(data);
            hide_show();
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_workitems_count_data = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_job_workitems_count_data = false;
        ducc_error(fname, err);
    }
}

function ducc_init_job_workitems_data() {
    var fname = "ducc_init_job_workitems_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#workitems_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_workitems_data = +new Date() - ms_reload_min;
var wip_job_workitems_data = false;

function ducc_load_job_workitems_data() {
    var fname = "ducc_load_job_workitems_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_workitems_data + ms_reload_min) {
        return;
    }
    ms_load_job_workitems_data = ms_now;
    if(wip_job_workitems_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_workitems_data = true;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_workitems_area").html(data);
        var servlet = "/ducc-servlet/job-workitems-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_workitems_data = false;
            $("#workitems_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_workitems_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_workitems_data = false;
            data = "";
            $("#loading_workitems_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_job_workitems_data = false;
        data = "";
        $("#loading_workitems_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_job_performance_data() {
    var fname = "ducc_init_job_performance_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#performance_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_performance_data = +new Date() - ms_reload_min;
var wip_job_performance_data = false;   

function ducc_load_job_performance_data() {
    var fname = "ducc_load_job_performance_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_performance_data + ms_reload_min) {
        return;
    }
    ms_load_job_performance_data = ms_now;
    if(wip_job_performance_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_performance_data = true;    
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_performance_area").html(data);
        var servlet = "/ducc-servlet/job-performance-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_performance_data = false;
            $("#performance_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_performance_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_performance_data = false;
            data = "";
            $("#loading_performance_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_job_performance_data = false;
        data = "";
        $("#loading_performance_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_job_specification_data() {
    var fname = "ducc_init_job_specification_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#specification_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_specification_data = +new Date() - ms_reload_min;
var wip_job_specification_data = false;

function ducc_load_job_specification_data() {
    var fname = "ducc_load_job_specification_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_specification_data + ms_reload_min) {
        return;
    }
    ms_load_job_specification_data = ms_now;
    if(wip_job_specification_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_specification_data = true;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_specification_area").html(data);
        var servlet = "/ducc-servlet/job-specification-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_specification_data = false;
            $("#specification_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_specification_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('specification_table'));
            }
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_specification_data = false;
            data = "";
            $("#loading_specification_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_job_specification_data = false;
        data = "";
        $("#loading_specification_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_job_files_data() {
    var fname = "ducc_init_job_files_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    } catch (err) {
         ducc_error(fname, err);
    }
}

var ms_load_job_files_data = +new Date() - ms_reload_min;
var wip_job_files_data = false;

function ducc_load_job_files_data() {
    var fname = "ducc_load_job_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_files_data + ms_reload_min) {
        return;
    }
    ms_load_job_files_data = ms_now;
    if(wip_job_files_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_files_data = true;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        var servlet = "/ducc-servlet/job-files-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_files_data = false;
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('files_table'));
            }
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_files_data = false;
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });        
    } catch (err) {
        wip_job_files_data = false;
        data = "";
        $("#loading_files_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_reservation_specification_data() {
    var fname = "ducc_init_reservation_specification_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#specification_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}


var ms_load_reservation_specification_data = +new Date() - ms_reload_min;
var wip_reservation_specification_data = false;

function ducc_load_reservation_specification_data() {
    var fname = "ducc_load_reservation_specification_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_specification_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_specification_data = ms_now;
    if(wip_reservation_specification_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_specification_data = true;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_specification_area").html(data);
        var servlet = "/ducc-servlet/reservation-specification-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_specification_data = false;
            $("#specification_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_specification_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('specification_table'));
            }
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_specification_data = false;
            data = "";
            $("#loading_specification_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_reservation_specification_data = false;
        data = "";
        $("#loading_specification_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_reservation_files_data() {
    var fname = "ducc_init_reservation_files_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_reservation_files_data = +new Date() - ms_reload_min;
var wip_reservation_files_data = false;

function ducc_load_reservation_files_data() {
    var fname = "ducc_load_reservation_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_files_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_files_data = ms_now;
    if(wip_reservation_files_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_files_data = true;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        var servlet = "/ducc-servlet/reservation-files-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_files_data = false;
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('files_table'));
            }
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_files_data = false;
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });        
    } catch (err) {
        wip_reservation_files_data = false;
        data = "";
        $("#loading_files_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_service_registry_data() {
    var fname = "ducc_init_service_registry_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#registry_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_service_registry_data = +new Date() - ms_reload_min;
var wip_service_registry_data = false;

function ducc_load_service_registry_data() {
    var fname = "ducc_load_service_registry_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_registry_data + ms_reload_min) {
        return;
    }
    ms_load_service_registry_data = ms_now;
    if(wip_service_registry_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_registry_data = true;
    try {
        var servlet = "/ducc-servlet/service-registry-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_registry_data = false;
            $("#registry_data_area").html(data);
            hide_show();
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_service_registry_data = false;
            ducc_console_fail(fname, textStatus);
        });            
    } catch (err) {
        wip_service_registry_data = false;
        ducc_error(fname, err);
    }
}

var ms_service_update_form_button = +new Date() - ms_reload_min;
var wip_service_update_form_button = false;

function ducc_service_update_form_button() {
    var fname = "ducc_service_update_form_button";
    var data = null;
        var ms_now = +new Date();
    if (ms_now < ms_service_update_form_button + ms_reload_min) {
        return;
    }
    ms_service_update_form_button = ms_now;
    if(wip_service_update_form_button) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_update_form_button = true;
    try {
        var servlet = "/ducc-servlet/service-update-get-form-button" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_update_form_button = false;
            $("#service_update_form_button").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_service_update_form_button = false;
            ducc_console_fail(fname, textStatus);
        });            
    } catch (err) {
        wip_service_update_form_button = false;
        ducc_error(fname, err);
    }
}

function ducc_init_service_deployments_data() {
    var fname = "ducc_init_service_deployments_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#deployments_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_service_deployments_data = +new Date() - ms_reload_min;
var wip_service_deployments_data = false;

function ducc_load_service_deployments_data() {
    var fname = "ducc_load_service_deployments_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_deployments_data + ms_reload_min) {
        return;
    }
    ms_load_service_deployments_data = ms_now;
    if(wip_service_deployments_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_deployments_data = true;
    try {
        var servlet = "/ducc-servlet/service-deployments-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_deployments_data = false;
            $("#deployments_list_area").html(data);
            ducc_cluetips();
            hide_show();
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_service_deployments_data = false;
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
        wip_service_deployments_data = false;
        ducc_error(fname, err);
    }
}

function ducc_init_service_files_data() {
    var fname = "ducc_init_service_files_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#files_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_service_files_data = +new Date() - ms_reload_min;
var wip_service_files_data = false;

function ducc_load_service_files_data() {
    var fname = "ducc_load_service_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_files_data + ms_reload_min) {
        return;
    }
    ms_load_service_files_data = ms_now;
    if(wip_service_files_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_files_data = true;
    try {
        var servlet = "/ducc-servlet/service-files-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_files_data = false;
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            //var table_style = ducc_preferences_get("table_style");
            //if (table_style == "scroll") {
            //    sorttable.makeSortable(document.getElementById('files_table'));
            //}
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_service_files_data = false;
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
        wip_service_files_data = false;
        data = "";
        $("#loading_files_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_service_history_data() {
    var fname = "ducc_init_service_history_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#history_data_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_service_history_data = +new Date() - ms_reload_min;
var wip_service_history_data = false;

function ducc_load_service_history_data() {
    var fname = "ducc_load_service_history_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_history_data + ms_reload_min) {
        return;
    }
    ms_load_service_history_data = ms_now;
    if(wip_service_history_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_service_history_data = true;
    try {
        var servlet = "/ducc-servlet/service-history-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_history_area").html(data);
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_service_history_data = false;
            $("#history_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_history_area").html(data);
            //var table_style = ducc_preferences_get("table_style");
            //if (table_style == "scroll") {
            //    sorttable.makeSortable(document.getElementById('history_table'));
            //}
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            wip_service_history_data = false;
            data = "";
            $("#history_data_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
        wip_service_history_data = false;
        data = "";
        $("#loading_history_area").html(data);
        ducc_error(fname, err);
    }
}

// Show or hide one of the buttons on a specification page
// Set the initial value from a cookie and define the toggle functions

function hide_show_button(index) {
    var fname = "hide_show_button";
    try {
        var buttondata = ducc_appl("showhidebutton"+index);
        var c_value = ducc_get_cookie(buttondata);
        if (c_value == null) {
            c_value = "hide";
        }
        if (c_value == "hide") {
            $('div.showdata'+index).hide();
            $('div.hidedata'+index).show();
        }
        if (c_value == "show") {
            $('div.showdata'+index).show();
            $('div.hidedata'+index).hide();
        }
        $('#showbutton'+index).click(function() {
            $('div.showdata'+index).show();
            $('div.hidedata'+index).hide();
            ducc_put_cookie(buttondata, "show")
        });
        $('#hidebutton'+index).click(function() {
            $('div.showdata'+index).hide();
            $('div.hidedata'+index).show();
            ducc_put_cookie(buttondata, "hide")
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

// Define the show/hide functions for the (possibly) 4 buttons on a specification page
function hide_show() {
    var fname = "hide_show";
    try {
		hide_show_button(0);
		hide_show_button(1);
		hide_show_button(2);
		hide_show_button(3);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_processes_data = +new Date() - ms_reload_min;
var wip_job_processes_data = false;

function ducc_load_job_processes_data() {
    var fname = "ducc_load_job_processes_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_processes_data + ms_reload_min) {
        return;
    }
    ms_load_job_processes_data = ms_now;
    if(wip_job_processes_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_processes_data = true;
    try {
        var servlet = "/ducc-servlet/job-processes-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_processes_area").html(data); 
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_processes_data = false;
            $("#processes_list_area").html(data);
            ducc_cluetips();
            hide_show();
            data = "";
            $("#loading_processes_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            wip_job_processes_data = false;
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                    
    } catch (err) {
        wip_job_processes_data = false;
        data = "";
        $("#loading_processes_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_job_processes_data() {
    var fname = "ducc_init_job_processes_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#processes_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_reservation_processes_data = +new Date() - ms_reload_min;
var wip_reservation_processes_data = false;

function ducc_load_reservation_processes_data() {
    var fname = "ducc_load_reservation_processes_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_processes_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_processes_data = ms_now;
    if(wip_reservation_processes_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_processes_data = true;
    try {
        var servlet = "/ducc-servlet/reservation-processes-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_processes_area").html(data); 
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_processes_data = false;
            $("#processes_list_area").html(data);
            ducc_cluetips();
            hide_show();
            data = "";
            $("#loading_processes_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_processes_data = false;
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                  
    } catch (err) {
        wip_reservation_processes_data = false;
        data = "";
        $("#loading_processes_area").html(data);
        ducc_error(fname, err);
    }
}

function ducc_init_reservation_processes_data() {
    var fname = "ducc_init_reservation_processes_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#processes_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_system_machines_data = +new Date() - ms_reload_min;

function ducc_load_machines_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_system_machines_data + ms_reload_min) {
        return;
    }
    ms_load_system_machines_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_machines_data()
    } else {
        ducc_load_classic_machines_data()
    }
}

var wip_classic_machines_data = false;

function ducc_load_classic_machines_data() {
    var fname = "ducc_load_classic_machines_data";
    var data = null;
    if(wip_classic_machines_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_classic_machines_data = true;
    try {
        var servlet = "/ducc-servlet/classic-system-machines-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_classic_machines_data = false;
            $("#machines_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_classic_machines_data = false;
            ducc_console_fail(fname, textStatus);
        });                      
    } catch (err) {
        wip_classic_machines_data = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_machines_data() {
    var fname = "ducc_load_scroll_machines_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-machines", ducc_load_scroll_machines_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_machines_callback() {
    var fname = "ducc_load_scroll_machines_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_machines_data() {
    var fname = "ducc_init_machines_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var wip_reservation_form_button = false;

function ducc_reservation_form_button() {
    var fname = "ducc_reservation_form_button";
    var data = null;
    if(wip_reservation_form_button) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_form_button = true;
    try {
        var servlet = "/ducc-servlet/reservation-get-form-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_form_button = false;
            $("#reservation_form_button").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_form_button = false;
            ducc_console_fail(fname, textStatus);
        });                      
    } catch (err) {
        wip_reservation_form_button = false;
        ducc_error(fname, err);
    }
}

function ducc_load_reservations_head() {
    ducc_reservations_max_records();
    ducc_reservations_users();
}

var ms_load_reservations_data = +new Date() - ms_reload_min;

function ducc_load_reservations_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_reservations_data + ms_reload_min) {
        return;
    }
    ms_load_reservations_data = ms_now;
    ducc_reservation_form_button();
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_reservations_data()
    } else {
        ducc_load_classic_reservations_data()
    }
}

var wip_classic_reservations_data = false;

function ducc_load_classic_reservations_data() {
    var fname = "ducc_load_classic_reservations_data";
    var data = null;
    if(wip_classic_reservations_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_classic_reservations_data = true;
    try {
        var servlet = "/ducc-servlet/classic-reservations-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_classic_reservations_data = false;
            $("#reservations_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_classic_reservations_data = false;
            ducc_console_fail(fname, textStatus);
        });                 
    } catch (err) {
        wip_classic_reservations_data = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_reservations_data() {
    var fname = "ducc_load_scroll_reservations_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-reservations", ducc_load_scroll_reservations_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_reservations_callback() {
    var fname = "ducc_load_scroll_reservations_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_reservations_data() {
    var fname = "ducc_init_reservations_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#reservations_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_reservation_scheduling_classes_data = +new Date() - ms_reload_min;
var wip_reservation_scheduling_classes = false;

function ducc_load_reservation_scheduling_classes() {
    var fname = "ducc_load_reservation_scheduling_classes";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_scheduling_classes_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_scheduling_classes_data = ms_now;
    if(wip_reservation_scheduling_classes) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_scheduling_classes = true;
    try {
        var servlet = "/ducc-servlet/reservation-scheduling-classes";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_scheduling_classes = false;
            $("#scheduling_class_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_scheduling_classes = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_reservation_scheduling_classes = false;
        ducc_error(fname, err);
    }
}

var wip_reservation_memory_units = false;

function ducc_load_reservation_memory_units() {
    var fname = "ducc_load_reservation_memory_units";
    var data = null;
    if(wip_reservation_memory_units) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_memory_units = true;
    try {
        var servlet = "/ducc-servlet/reservation-memory-units";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_memory_units = false;
            $("#memory_units_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_memory_units = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_reservation_memory_units = false;
        ducc_error(fname, err);
    }
}

var wip_reservation_submit_button = false;

function ducc_load_reservation_submit_button() {
    var fname = "ducc_load_reservation_submit_button";
    var data = null;
    if(wip_reservation_submit_button) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_reservation_submit_button = true;
    try {
        var servlet = "/ducc-servlet/reservation-get-submit-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_reservation_submit_button = false;
            $("#reservation_submit_button_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_reservation_submit_button = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_reservation_submit_button = false;
        ducc_error(fname, err);
    }
}

function ducc_load_submit_reservation_data() {
    var fname = "ducc_load_submit_reservation_data";
    try {
        ducc_load_reservation_scheduling_classes();
        ducc_load_reservation_memory_units();
        ducc_load_reservation_submit_button();
        ducc_load_common();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_submit_reservation_data() {
    var fname = "ducc_init_submit_reservation_data";
    var data = null;
    try {
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var wip_job_form = false;

function ducc_load_job_form() {
    var fname = "ducc_load_job_form";
    var data = null;
    if(wip_job_form) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_form = true;
    try {
        var servlet = "/ducc-servlet/job-submit-form";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_form = false;
            $("#job_submit_form_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_form = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_job_form = false;
        ducc_error(fname, err);
    }
}

var wip_job_submit_button = false;

function ducc_load_job_submit_button() {
    var fname = "ducc_load_job_submit_button";
    var data = null;
    if(wip_job_submit_button) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_job_submit_button = true;
    try {
        var servlet = "/ducc-servlet/job-get-submit-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_job_submit_button = false;
            $("#job_submit_button_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_job_submit_button = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_job_submit_button = false;
        ducc_error(fname, err);
    }
}

var wip_system_admin_admin_data = false;

function ducc_load_system_admin_admin_data() {
    var fname = "ducc_load_system_admin_admin_data";
    var data = null;
    if(wip_system_admin_admin_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_system_admin_admin_data = true;
    try {
        var servlet = "/ducc-servlet/system-admin-admin-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_system_admin_admin_data = false;
            $("#system_administration_administrators_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_system_admin_admin_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_system_admin_admin_data = false;
        ducc_error(fname, err);
    }
}

var wip_system_admin_control_data = false;

function ducc_load_system_admin_control_data() {
    var fname = "ducc_load_system_admin_control_data";
    var data = null;
    if(wip_system_admin_control_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_system_admin_control_data = true;
    try {
        var servlet = "/ducc-servlet/system-admin-control-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_system_admin_control_data = false;
            $("#system_administration_control_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_system_admin_control_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_system_admin_control_data = false;
        ducc_error(fname, err);
    }
}

var ms_load_system_administration_data = +new Date() - ms_reload_min;

function ducc_load_system_administration_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_system_administration_data + ms_reload_min) {
        return;
    }
    ms_load_system_administration_data = ms_now;
    ducc_load_system_admin_admin_data();
    ducc_load_system_admin_control_data();
    ducc_load_common();
}

function ducc_init_system_administration_data() {
    var fname = "ducc_init_system_administration_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#system_administration_administrators_area").html(data);
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#system_administration_quiesce_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_system_classes_data = +new Date() - ms_reload_min;

function ducc_load_system_classes_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_system_classes_data + ms_reload_min) {
        return;
    }
    ms_load_system_classes_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_system_classes_data()
    } else {
        ducc_load_classic_system_classes_data()
    }
}

var wip_classic_system_classes_data = false;

function ducc_load_classic_system_classes_data() {
    var fname = "ducc_load_classic_system_classes_data";
    var data = null;
    if(wip_classic_system_classes_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_classic_system_classes_data = true;
    try {
        var servlet = "/ducc-servlet/classic-system-classes-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_classic_system_classes_data = false;
            $("#system_classes_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_classic_system_classes_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_classic_system_classes_data = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_classes_data() {
    var fname = "ducc_load_scroll_system_classes_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-classes", ducc_load_scroll_system_classes_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_classes_callback() {
    var fname = "ducc_load_scroll_system_classes_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_system_classes_data() {
    var fname = "ducc_init_system_classes_data";
    var data = null;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_button_show_agents() {
    var fname = "ducc_button_show_agents";
    try {
        var agents = ducc_appl("agents");
        var c_value = "show";
        ducc_put_cookie(agents, c_value);
        document.getElementById("showbutton").style.display = 'none';
        document.getElementById("hidebutton").style.display = 'block';
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_show_agents() {
    var fname = "ducc_show_agents";
    try {
        ducc_button_show_agents();
        ducc_refresh("system-daemons");
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_button_hide_agents() {
    var fname = "ducc_button_hide_agents";
    try {
        var agents = ducc_appl("agents");
        var c_value = "hide";
        ducc_put_cookie(agents, c_value);
        document.getElementById("showbutton").style.display = 'block';
        document.getElementById("hidebutton").style.display = 'none';
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_hide_agents() {
    var fname = "ducc_hide_agents";
    try {
        ducc_button_hide_agents();
        ducc_refresh("system-daemons");
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_default_agents() {
    var fname = "ducc_default_agents";
    try {
        var agents = ducc_appl("agents");
        var c_value = ducc_get_cookie(agents);
        if (c_value == "hide") {
            ducc_button_hide_agents();
        } else if (c_value == "show") {
            ducc_button_show_agents();
        } else {
            ducc_button_hide_agents();
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_system_daemons_data = +new Date() - ms_reload_min;

function ducc_load_system_daemons_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_system_daemons_data + ms_reload_min) {
        return;
    }
    ms_load_system_daemons_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_system_daemons_data()
    } else {
        ducc_load_classic_system_daemons_data()
    }
    ducc_default_agents();
}

var wip_classic_system_daemons_data = false;

function ducc_load_classic_system_daemons_data() {
    var fname = "ducc_load_classic_system_daemons_data";
    var data = null;
    if(wip_classic_system_daemons_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_classic_system_daemons_data = true;
    try {
        var servlet = "/ducc-servlet/classic-system-daemons-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_classic_system_daemons_data = false;
            $("#system_daemons_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_classic_system_daemons_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_classic_system_daemons_data = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_daemons_data() {
    var fname = "ducc_load_scroll_system_daemons_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-daemons", ducc_load_scroll_system_daemons_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_daemons_callback() {
    var fname = "ducc_load_scroll_system_daemons_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_system_daemons_data() {
    var fname = "ducc_init_system_daemons_data";
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_system_broker_data = +new Date() - ms_reload_min;

function ducc_load_system_broker_data() {
    var ms_now = +new Date();
    if (ms_now < ms_load_system_broker_data + ms_reload_min) {
        return;
    }
    ms_load_system_broker_data = ms_now;
    var table_style = ducc_preferences_get("table_style");
    if (table_style == "scroll") {
        ducc_load_scroll_system_broker_data()
    } else {
        ducc_load_classic_system_broker_data()
    }
}

var wip_classic_system_broker_data = false;

function ducc_load_classic_system_broker_data() {
    var fname = "ducc_load_classic_system_broker_data";
    var data = null;
    if(wip_classic_system_broker_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_classic_system_broker_data = true;
    try {
        var servlet = "/ducc-servlet/classic-system-broker-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_classic_system_broker_data = false;
            $("#system_broker_list_area").html(data);
            data = null;
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            wip_classic_system_broker_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_classic_system_broker_data = false;
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_broker_data() {
    var fname = "ducc_load_scroll_system_broker_data";
    try {
        oTable.fnReloadAjax("/ducc-servlet/json-format-aaData-broker", ducc_load_scroll_system_broker_callback);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_scroll_system_broker_callback() {
    var fname = "ducc_load_scroll_system_broker_callback";
    try {
        ducc_load_common();
        ducc_cluetips();
        oTable.fnAdjustColumnSizing();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init_system_broker_data() {
    var fname = "ducc_init_system_broker_data";
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">"
        $("#system_broker_list_area").html(data);
        data = "...?"
        $("#timestamp_area").html(data);
        data = "...?"
        $("#authentication_area").html(data);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_init(type) {
    var fname = "ducc_init";
    try {
        ducc_identity();
        ducc_version();
        ducc_home();
        ducc_links();
        ducc_cookies();
        if (type == "viz") {
            ducc_init_viz_data();
            ducc_load_viz_head();
            ducc_load_viz_data();
        }
        if (type == "jobs") {
            $(document).keypress(function(e) {
                if (e.which == 13) {
                    ducc_load_jobs_head();
                    ducc_load_jobs_data();
                }
            });
            ducc_init_jobs_data();
            ducc_load_jobs_head();
            ducc_load_jobs_data();
        }
        if (type == "services") {
            $(document).keypress(function(e) {
                if (e.which == 13) {
                    ducc_load_services_head();
                    ducc_load_services_data();
                }
            });
            ducc_init_services_data();
            ducc_load_services_head();
            ducc_load_services_data();
        }
        if (type == "job-details") {
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
        if (type == "uima-initialization-report") {
            uima_initialization_report();
        }
        if (type == "reservation-details") {
            ducc_init_reservation_processes_data();
            ducc_init_reservation_specification_data();
            ducc_init_reservation_files_data();
            ducc_load_reservation_processes_data();
            ducc_load_reservation_specification_data();
            ducc_load_reservation_files_data();
        }
        if (type == "service-details") {
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
        if (type == "system-machines") {
            ducc_init_machines_data();
            ducc_load_machines_data();
        }
        if (type == "reservations") {
            $(document).keypress(function(e) {
                if (e.which == 13) {
                    ducc_load_reservations_head();
                    ducc_load_reservations_data();
                }
            });
            ducc_init_reservations_data();
            ducc_load_reservations_head();
            ducc_load_reservations_data();
        }
        if (type == "submit-reservation") {
            ducc_init_submit_reservation_data();
            ducc_load_submit_reservation_data();
        }
        if (type == "system-administration") {
            ducc_init_system_administration_data();
            ducc_load_system_administration_data();
        }
        if (type == "system-classes") {
            ducc_init_system_classes_data();
            ducc_load_system_classes_data();
        }
        if (type == "system-daemons") {
            ducc_init_system_daemons_data();
            ducc_load_system_daemons_data();
        }
        if (type == "system-broker") {
            ducc_init_broker_summary_data();
            ducc_init_system_broker_data();
            ducc_load_broker_summary_data();
            ducc_load_system_broker_data();
        }
        if (type == "authentication-login") {
            ducc_init_common();
            ducc_load_common();
            ducc_password_checked();
            $(document).keypress(function(e) {
                if (e.which == 13) {
                    ducc_submit_login();
                }
            });
        }
        if (type == "authentication-logout") {
            ducc_init_common();
            ducc_load_common();
            $(document).keypress(function(e) {
                if (e.which == 13) {
                    ducc_logout();
                }
            });
        }
        $.getScript("./js/ducc.local.js", function() {
            ducc_init_local(type);
        });
        var table_style = ducc_preferences_get("table_style");
        display_table_style = table_style;
        ducc_timed_loop(type);
    } catch (err) {
        ducc_error(fname, err);
    }
}

/*
 * transition function for use in converting 
 * cookie names from ducc:xxxx to DUCCxxxx
 */
// @Deprecated 
function ducc_transform_all_cookies() {
    var fname = "ducc_transform_all_cookies";
    try {
    	var pairs = document.cookie.split(";");
    	var cookies = {};
    	for (var i=0; i<pairs.length; i++){
    		var nvp = pairs[i].split("=");
    		if(nvp.length == 2) {
    			var name = nvp[0].trim();
        		var value = nvp[1].trim();
        		if(name.startsWith("ducc:")) {
        			var nameSuffix = name.substring(5);
        			var nameModern = "DUCC"+nameSuffix;
        			// delete bad cookie
        			document.cookie = name +'=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;';
        			// create good cookie
        			ducc_put_cookie(nameModern, value);
        		}
    		}
    		else if(nvp.length == 1) {
    			var name = nvp[0].trim();
    			if(name.startsWith("ducc:")) {
    				// delete bad cookie
        			document.cookie = name +'=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;';
    			}
    		}
    	}
    } catch (err) {
    	ducc_error(fname, err);
    }
}

function ducc_cookies() {
    var fname = "ducc_cookies";
    try {
    	ducc_transform_all_cookies();
        var refreshmode = ducc_appl("refreshmode");
        var c_value = ducc_get_cookie(refreshmode);
        if (c_value == "automatic") {
            document.duccform.refresh[0].checked = false;
            document.duccform.refresh[1].checked = true;
        } else if (c_value == "manual") {
            document.duccform.refresh[0].checked = true;
            document.duccform.refresh[1].checked = false;
        } else {
            document.duccform.refresh[0].checked = false;
            document.duccform.refresh[1].checked = true;
            c_value = "automatic";
            ducc_put_cookie(refreshmode, c_value);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

var wip_uima_initialization_report_summary = false;

function uima_initialization_report_summary() {
    var fname = "uima_initialization_report_summary";
    var data = null;
    if(wip_uima_initialization_report_summary) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_uima_initialization_report_summary = true;
    try {
        var servlet = "/ducc-servlet/uima-initialization-report-summary" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_uima_initialization_report_summary = false;
            $("#uima_initialization_report_summary").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_uima_initialization_report_summary = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_uima_initialization_report_summary = false;
        ducc_error(fname, err);
    }
}

var wip_uima_initialization_report_data = false;

function uima_initialization_report_data() {
    var fname = "uima_initialization_report_data";
    var data = null;
    if(wip_uima_initialization_report_data) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_uima_initialization_report_data = true;
    try {
        var servlet = "/ducc-servlet/uima-initialization-report-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_uima_initialization_report_data = false;
            $("#uima_initialization_report_data").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_uima_initialization_report_data = false;
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        wip_uima_initialization_report_data = false;
        ducc_error(fname, err);
    }
}

var ms_uima_initialization_report = +new Date() - ms_reload_min;

function uima_initialization_report(name) {
    var fname = "uima_initialization_report";
    var ms_now = +new Date();
    if (ms_now < ms_uima_initialization_report + ms_reload_min) {
        return;
    }
    ms_uima_initialization_report = ms_now;
    try {
        uima_initialization_report_summary();
        uima_initialization_report_data();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_appl(name) {
    var fname = "ducc_appl";
    try {
        var appl = "DUCC";
        return appl + name;
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_jobs_max_records() {
    try {
        var d_value = "16";
        var x_value = "1";
        var y_value = "4096";
        var jobsmax = ducc_appl("jobsmax");
        //
        var c_value = ducc_get_cookie(jobsmax);
        var r_value = document.form_selectors.maxrecs_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(jobsmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.maxrecs_input.value = c_value;
            //$.jGrowl(" max records: "+c_value);
            return;
        }
        //
        n_value = 1 * r_value;
        if (isNaN(n_value)) {
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records, invalid: " + r_value);
            return;
        }
        r_value = 1 * r_value;
        x_value = 1 * x_value;
        y_value = 1 * y_value;
        if (r_value < x_value) {
            $.jGrowl(" max records, too small: " + r_value + " < " + x_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value > y_value) {
            $.jGrowl(" max records, too large: " + r_value + " > " + y_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        //
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(jobsmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records: " + c_value);
            return;
        }
    } catch (err) {
        throw err;
        //ducc_error("ducc_jobs_max_records",err);
    }
}

function ducc_jobs_users() {
    try {
        var jobsusers = ducc_appl("jobsusers");
        var d_value = "";
        var c_value = ducc_get_cookie(jobsusers);
        var r_value = document.form_selectors.users_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(jobsusers, c_value);
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(jobsusers, c_value);
            document.form_selectors.users_input.value = c_value;
            $.jGrowl(" users: " + c_value);
            return;
        }
        return;
    } catch (err) {
        throw err;
        //ducc_error("ducc_jobs_users",err);
    }
}

function ducc_services_max_records() {
    try {
        var d_value = "16";
        var x_value = "1";
        var y_value = "4096";
        var servicesmax = ducc_appl("servicesmax");
        //
        var c_value = ducc_get_cookie(servicesmax);
        var r_value = document.form_selectors.maxrecs_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(servicesmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.maxrecs_input.value = c_value;
            //$.jGrowl(" max records: "+c_value);
            return;
        }
        //
        n_value = 1 * r_value;
        if (isNaN(n_value)) {
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records, invalid: " + r_value);
            return;
        }
        r_value = 1 * r_value;
        x_value = 1 * x_value;
        y_value = 1 * y_value;
        if (r_value < x_value) {
            $.jGrowl(" max records, too small: " + r_value + " < " + x_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value > y_value) {
            $.jGrowl(" max records, too large: " + r_value + " > " + y_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        //
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(servicesmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records: " + c_value);
            return;
        }
    } catch (err) {
        throw err;
        //ducc_error("ducc_services_max_records",err);
    }
}

function ducc_services_users() {
    try {
        var servicesusers = ducc_appl("servicesusers");
        var d_value = "";
        var c_value = ducc_get_cookie(servicesusers);
        var r_value = document.form_selectors.users_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(servicesusers, c_value);
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(servicesusers, c_value);
            document.form_selectors.users_input.value = c_value;
            $.jGrowl(" users: " + c_value);
            return;
        }
        return;
    } catch (err) {
        throw err;
        //ducc_error("ducc_services_users",err);
    }
}

function ducc_reservations_max_records() {
    try {
        var d_value = "16";
        var x_value = "1";
        var y_value = "4096";
        var reservationsmax = ducc_appl("reservationsmax");
        //
        var c_value = ducc_get_cookie(reservationsmax);
        var r_value = document.form_selectors.maxrecs_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(reservationsmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.maxrecs_input.value = c_value;
            //$.jGrowl(" max records: "+c_value);
            return;
        }
        //
        n_value = 1 * r_value;
        if (isNaN(n_value)) {
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records, invalid: " + r_value);
            return;
        }
        r_value = 1 * r_value;
        x_value = 1 * x_value;
        y_value = 1 * y_value;
        if (r_value < x_value) {
            $.jGrowl(" max records, too small: " + r_value + " < " + x_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        if (r_value > y_value) {
            $.jGrowl(" max records, too large: " + r_value + " > " + y_value);
            document.form_selectors.maxrecs_input.value = c_value;
            return;
        }
        //
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(reservationsmax, c_value);
            document.form_selectors.maxrecs_input.value = c_value;
            $.jGrowl(" max records: " + c_value);
            return;
        }
    } catch (err) {
        throw err;
        //ducc_error("ducc_reservations_max_records",err);
    }
}

function ducc_reservations_users() {
    try {
        var reservationsusers = ducc_appl("reservationsusers");
        var d_value = "";
        var c_value = ducc_get_cookie(reservationsusers);
        var r_value = document.form_selectors.users_input.value;
        if (c_value == null) {
            c_value = d_value;
            ducc_put_cookie(reservationsusers, c_value);
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (r_value == "default") {
            document.form_selectors.users_input.value = c_value;
            return;
        }
        if (c_value != r_value) {
            c_value = r_value;
            ducc_put_cookie(reservationsusers, c_value);
            document.form_selectors.users_input.value = c_value;
            $.jGrowl(" users: " + c_value);
            return;
        }
        return;
    } catch (err) {
        throw err;
        //ducc_error("ducc_reservations_users",err);
    }
}

var refresh_page_busy = false;
var refresh_page_busy_count = 0;

function ducc_refresh_page(type) {
    var fname = "ducc_refresh_page";
    if(refresh_page_busy) {
        refresh_page_busy_count += 1;
        var message = fname + ".warn: " + "busyCount = " + refresh_page_busy_count;
        ducc_console_warn(message);
        return;
    }
    else {
        refresh_page_busy = true;
    }
    ducc_cookies();
    try {
        var table_style = ducc_preferences_get("table_style");
        if (display_table_style == table_style) {
            ducc_update_page(type);
        } else {
            display_table_style = table_style;
            window.location.reload();
        }
    } catch (err) {
        var message = fname + ".warn: " + "caught = " + err;
        ducc_console_warn(message);
    }
    refresh_page_busy = false;
    refresh_page_busy_count = 0;
}

function ducc_update_page(type) {
    var fname = "ducc_update_page";
    try {
        if (type == "viz") {
            ducc_load_viz_head();
            ducc_load_viz_data();
        }
        if (type == "jobs") {
            ducc_load_jobs_head();
            ducc_load_jobs_data();
        }
        if (type == "services") {
            ducc_load_services_head();
            ducc_load_services_data();
        }
        if (type == "reservations") {
            ducc_load_reservations_head();
            ducc_load_reservations_data();
        }
        if (type == "job-details") {
            ducc_load_job_workitems_count_data();
            ducc_load_job_processes_data();
            ducc_load_job_workitems_data();
            ducc_load_job_performance_data();
            ducc_load_job_specification_data();
            ducc_load_job_files_data();
        }
        if (type == "reservation-details") {
            ducc_load_reservation_specification_data();
            ducc_load_reservation_processes_data();
            ducc_load_reservation_files_data();
        }
        if (type == "service-details") {
            ducc_load_service_history_data();
            ducc_load_service_files_data();
            ducc_load_service_registry_data();
            ducc_load_service_deployments_data();
            ducc_load_service_summary_data();
            ducc_service_update_form_button();
        }
        if (type == "system-machines") {
            ducc_load_machines_data();
        }
        if (type == "system-administration") {
            ducc_load_system_administration_data();
        }
        if (type == "system-daemons") {
            ducc_load_system_daemons_data();
        }
        if (type == "system-broker") {
            ducc_load_broker_summary_data();
            ducc_load_system_broker_data();
        }
        if (type == "system-classes") {
            ducc_load_system_classes_data();
        }
        $.getScript("./js/ducc.local.js", function() {
            ducc_update_page_local(type);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_refresh_stopped(type) {
    var fname = "ducc_refresh_stopped";
    ducc_console_enter(fname);
    try {
        document.getElementById("refreshbutton").style.display = 'block';
    } catch (err) {
    }
    try {
        document.getElementById("loading").style.display = 'none';
    } catch (err) {
    }
    ducc_console_exit(fname);
}

function ducc_refresh_running(type) {
    var fname = "ducc_refresh_running";
    ducc_console_enter(fname);
    ducc_refresh_page(type);
    ducc_console_exit(fname);
}

function ducc_refresh_started(type) {
    var fname = "ducc_refresh_started";
    ducc_console_enter(fname);
    try {
        document.getElementById("refreshbutton").style.display = 'none';
    } catch (err) {
    }
    try {
        document.getElementById("loading").style.display = 'block';
    } catch (err) {
    }
    ducc_console_exit(fname);
}

var to_started = null;
var to_stopped = null;

function ducc_refresh(type) {
    var fname = "ducc_refresh";
    ducc_console_enter(fname);
    if(to_started != null) {
        clearTimeout(to_started);
    }
    to_started = setTimeout(function() {
        ducc_refresh_started(type);
        type = null
    }, 1);
    if(to_stopped != null) {
        clearTimeout(to_stopped);
    }
    to_stopped = setTimeout(function() {
        ducc_refresh_stopped(type);
        type = null
    }, 1001);
    ducc_refresh_running(type);
    ducc_console_exit(fname);
}

var wip_alerts = false;

function ducc_alerts() {
    var fname = "ducc_alerts";
    var data = null;
    if(wip_alerts) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_alerts = true;
    try {
        var servlet = "/ducc-servlet/alerts";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_alerts = false;
            target = "alerts_div";
            display = "initial";
            if(data != null) {
            	tdata = data.trim();
            	if(tdata.length == 0) {
            		display = "none";
            	}
            }
            document.getElementById(target).style.display = display;
            $("#alerts_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_alerts = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_alerts = false;
        ducc_error(fname, err);
    }
}

var wip_messages = false;

function ducc_messages() {
    var fname = "ducc_messages";
    var data = null;
    if(wip_messages) {
        ducc_console_warn(fname+" already in progress...")
        return;
    }
    wip_messages = true;
    try {
        var servlet = "/ducc-servlet/banner-message";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            wip_messages = false;
            target = "messages_div";
            display = "initial";
            if(data != null) {
            	tdata = data.trim();
            	if(tdata.length == 0) {
            		display = "none";
            	}
            }
            document.getElementById(target).style.display = display;
            $("#messages_area").html(data);
            data = null;
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            wip_messages = false;
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        wip_messages = false;
        ducc_error(fname, err);
    }
}

var to_timed_loop = null;

function ducc_timed_loop(type) {
    var fname = "ducc_timed_loop";
    ducc_console_enter(fname);
    try {
    	ducc_cookies();
    	ducc_alerts();
    	ducc_messages();
        var refreshmode = ducc_appl("refreshmode");
        var c_value = ducc_get_cookie(refreshmode);
        if (c_value == null) {
            c_value = "automatic";
            ducc_put_cookie(refreshmode, c_value);
        }
        if (c_value == "automatic") {
            ducc_refresh(type);
        }
        if(to_timed_loop != null) {
            clearTimeout(to_timed_loop);
        }
        to_timed_loop = setTimeout(function() {
            ducc_timed_loop(type);
            type = null
        }, 30000); // again
    } catch (err) {
        ducc_error(fname, err);
    }
    ducc_console_exit(fname);
}

function ducc_terminate_job(id) {
    var fname = "ducc_terminate_job";
    try {
        $.jGrowl(" Pending termination...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/job-cancel-request" + "?id=" + id,
            success: function(data) {
                $.jGrowl(data, {
                    life: 6000
                });
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_terminate_service(id) {
    var fname = "ducc_terminate_service";
    try {
        $.jGrowl(" Pending termination...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/service-cancel-request" + "?id=" + id,
            success: function(data) {
                $.jGrowl(data, {
                    life: 6000
                });
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_service_enable(id) {
    var fname = "ducc_service_enable";
    try {
        $.jGrowl(" Pending enable...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/service-enable-request" + "?id=" + id,
            success: function(data) {
                $.jGrowl(data, {
                    life: 6000
                });
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_service_stop(id) {
    var fname = "ducc_service_stop";
    try {
        $.jGrowl(" Pending stop...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/service-stop-request" + "?id=" + id,
            success: function(data) {
                $.jGrowl(data, {
                    life: 6000
                });
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_terminate_reservation(id) {
    var fname = "ducc_terminate_reservation";
    try {
        $.jGrowl(" Pending termination...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/reservation-cancel-request" + "?id=" + id,
            success: function(data) {
                $.jGrowl(data, {
                    life: 6000
                });
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_confirm_accept_jobs() {
    var fname = "ducc_confirm_accept_jobs";
    try {
        var result = confirm("System to accept job submits?");
        if (result == true) {
            ducc_accept_jobs();
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_accept_jobs(id) {
    var fname = "ducc_accept_jobs";
    try {
        $.jGrowl(" Pending jobs submit unblocking...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/jobs-control-request" + "?type=accept",
            success: function(data) {
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_confirm_block_jobs() {
    var fname = "ducc_confirm_block_jobs";
    try {
        var result = confirm("System to block job submits?");
        if (result == true) {
            ducc_block_jobs();
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_block_jobs(id) {
    var fname = "ducc_block_jobs";
    try {
        $.jGrowl(" Pending jobs submit blocking...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/jobs-control-request" + "?type=block",
            success: function(data) {
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_confirm_terminate_job(id) {
    var fname = "ducc_confirm_terminate_job";
    try {
        var result = confirm("Terminate job " + id + "?");
        if (result == true) {
            ducc_terminate_job(id);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_confirm_terminate_service(id) {
	ducc_confirm_terminate_reservation(id);
}

function ducc_confirm_service_enable(id) {
    var fname = "ducc_confirm_service_enable";
    try {
        var result = confirm("Enable service " + id + "?");
        if (result == true) {
            ducc_service_enable(id);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_confirm_service_stop(id) {
    var fname = "ducc_confirm_service_stop";
    try {
        var result = confirm("Stop service " + id + "?");
        if (result == true) {
            ducc_service_stop(id);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_confirm_terminate_reservation(id) {
    var fname = "ducc_confirm_terminate_reservation";
    try {
        var result = confirm("Terminate reservation " + id + "?");
        if (result == true) {
            ducc_terminate_reservation(id);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_logout() {
    var fname = "ducc_logout";
    try {
        $.jGrowl(" Pending logout...");
        $.ajax({
            url: "/ducc-servlet/user-logout",
            success: function(data) {
                setTimeout(function() {
                    window.close();
                }, 5000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 5000);
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_cancel_logout() {
    var fname = "ducc_cancel_logout";
    try {
        window.close();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_submit_login() {
    var fname = "ducc_submit_login";
    try {
        var url = document.forms[1].action;
        var userid = document.forms[1].userid.value
        var password = document.forms[1].password.value
        $.jGrowl(" Pending login...");
        $.ajax({
            type: "POST",
            url: url,
            data: $("#login").serialize(), // serializes the form's elements.
            success: function(data) {
                result = data.trim();
                if (result == "success") {
                    //$.jGrowl(" "+result[1]+"="+result[2]);
                    //$.jGrowl(" "+result[3]+"="+result[4]);
                    //ducc_put_cookie(result[1],result[2]);
                    //ducc_put_cookie(result[3],result[4]);
                    $.jGrowl(" " + "login success", {
                        theme: 'jGrowl-success'
                    });
                    setTimeout(function() {
                        window.close();
                    }, 5000);
                } else {
                    $.jGrowl(" " + "login failed", {
                        theme: 'jGrowl-error'
                    });
                    $.jGrowl(" " + data, {
                        life: 15000
                    });
                    setTimeout(function() {
                        window.close();
                    }, 15000);
                }
            }
        });
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_cancel_login() {
    var fname = "ducc_cancel_login";
    try {
        window.close();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_cancel_submit_reservation() {
    var fname = "ducc_cancel_submit_reservation";
    try {
        window.close();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_submit_reservation() {
    var fname = "ducc_submit_reservation";
    try {
        var e = document.getElementById("scheduling_class");
        var scheduling_class = e.options[e.selectedIndex].value;
        var e = document.getElementById("memory_size");
        var memory_size = e.value;
        var e = document.getElementById("memory_units");
        var memory_units = e.options[e.selectedIndex].value;
        var e = document.getElementById("description");
        var description = e.value;
        var e = document.getElementById("wait_for_result_yes");
        var wait_for_result = e.checked;
        if (wait_for_result) {
            document.getElementById("working_area").style.display = 'block';
            document.getElementById("submit_button").disabled = 'disabled';
            //
            document.getElementById("scheduling_class").disabled = 'disabled';
            document.getElementById("memory_size").disabled = 'disabled';
            document.getElementById("memory_units").disabled = 'disabled';
            document.getElementById("description").disabled = 'disabled';
            
            $.ajax({
                type: 'POST',
                async: false,
                url: "/ducc-servlet/reservation-submit-request",
                data: {
                    'scheduling_class': scheduling_class,
                    'memory_size': memory_size,
                    'description': description
                },
                success: function(data) {
                    $.jGrowl(data, {
                        life: 15000
                    });
                    setTimeout(function() {
                        window.close();
                    }, 15000);
                }
            });
            setTimeout(function() {
                window.close();
            }, 15000);

            document.getElementById("working_area").style.display = 'none';
        } else {
            $.jGrowl(" Pending allocation...");
            $.ajax({
                type: 'POST',
                url: "/ducc-servlet/reservation-submit-request",
                data: {
                    'scheduling_class': scheduling_class,
                    'memory_size': memory_size,
                    'description': description
                },
                success: function(data) {
                    setTimeout(function() {
                        window.close();
                    }, 5000);
                }
            });
            setTimeout(function() {
                window.close();
            }, 5000);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_update_service(name) {
    var fname = "ducc_update_service";
    try {
        var e = document.getElementById("autostart");
        var autostart = e.options[e.selectedIndex].value;
        var e = document.getElementById("instances");
        var instances = e.value;
        document.getElementById("update_button").disabled = 'disabled';
        $.ajax({
            type: 'POST',
            async: false,
            url: "/ducc-servlet/service-update-request",
            data: {
                'id': name,
                'autostart': autostart,
                'instances': instances
            },
            success: function(data) {
                $.jGrowl(data, {
                    life: 15000
                });
                setTimeout(function() {
                    window.close();
                }, 15000);
            }
        });
        setTimeout(function() {
            window.close();
        }, 15000);
        document.getElementById("update_button").disabled = '';
    } catch (err) {
        ducc_error(fname, err);
    }
    return false;
}

function ducc_put_cookie(name, value) {
    var fname = "ducc_put_cookie";
    try {
        var days = 365 * 31;
        ducc_put_cookie_timed(name, value, days);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_put_cookie_timed(name, value, days) {
    var fname = "ducc_put_cookie_timed";
    try {
        if (days) {
            var date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            var expires = "; expires=" + date.toGMTString();
        } else var expires = "";
        document.cookie = name + "=" + value + expires + "; path=/";
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_get_cookie(name) {
    var fname = "ducc_get_cookie";
    var cookie = null;
    try {
        var nameEQ = name + "=";
        var ca = document.cookie.split(';');
        for (var i = 0; i < ca.length; i++) {
            var c = ca[i];
            while (c.charAt(0) == ' ') c = c.substring(1, c.length);
            if (c.indexOf(nameEQ) == 0) {
                cookie = c.substring(nameEQ.length, c.length);
                break;
            }
        }
    } catch (err) {
        ducc_error(fname, err);
    }
    return cookie;
}

function ducc_remove_cookie(name) {
    var fname = "ducc_remove_cookie";
    try {
        ducc_put_cookie(name, "", -1);
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_reset() {
    var fname = "ducc_preferences_reset";
    try {
        var key = ducc_appl("table_style");
        var value = "classic";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        var key = ducc_appl("date_style");
        var value = "long";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        var key = ducc_appl("description_style");
        var value = "long";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        var key = ducc_appl("display_style");
        var value = "textual";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        var key = ducc_appl("filter_users_style");
        var value = "include";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        var key = ducc_appl("role");
        var value = "user";
        //alert("ducc_preferences_reset"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        //
        ducc_preferences();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_set(name, value) {
    var fname = "ducc_preferences_set";
    try {
        var key = ducc_appl(name);
        //alert("ducc_preferences_set"+" "+"key:"+key+" "+"value:"+value);
        ducc_put_cookie(key, value);
        ducc_preferences();
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_get(name) {
    var fname = "ducc_preferences_get";
    try {
        var key = ducc_appl(name);
        var value = null;
        value = ducc_get_cookie(key);
        //alert("ducc_preferences_get"+" "+"key:"+key+" "+"value:"+value);
        return value;
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_table_style() {
    var fname = "ducc_preferences_table_style";
    try {
        var key = ducc_appl("table_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "classic") {
            document.form_preferences.table_style[0].checked = true;
            document.form_preferences.table_style[1].checked = false;
        } else if (value == "scroll") {
            document.form_preferences.table_style[0].checked = false;
            document.form_preferences.table_style[1].checked = true;
        } else {
            value = "classic";
            ducc_put_cookie(key, value);
            document.form_preferences.table_style[0].checked = true;
            document.form_preferences.table_style[1].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_date_style() {
    var fname = "ducc_preferences_date_style";
    try {
        var key = ducc_appl("date_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "long") {
            document.form_preferences.date_style[0].checked = true;
            document.form_preferences.date_style[1].checked = false;
            document.form_preferences.date_style[2].checked = false;
        } else if (value == "medium") {
            document.form_preferences.date_style[0].checked = false;
            document.form_preferences.date_style[1].checked = true;
            document.form_preferences.date_style[2].checked = false;
        } else if (value == "short") {
            document.form_preferences.date_style[0].checked = false;
            document.form_preferences.date_style[1].checked = false;
            document.form_preferences.date_style[2].checked = true;
        } else {
            value = "long";
            ducc_put_cookie(key, value);
            document.form_preferences.date_style[0].checked = true;
            document.form_preferences.date_style[1].checked = false;
            document.form_preferences.date_style[2].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_description_style() {
    var fname = "ducc_preferences_description_style";
    try {
        var key = ducc_appl("description_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "long") {
            document.form_preferences.description_style[0].checked = true;
            document.form_preferences.description_style[1].checked = false;
        } else if (value == "short") {
            document.form_preferences.description_style[0].checked = false;
            document.form_preferences.description_style[1].checked = true;
        } else {
            value = "long";
            ducc_put_cookie(key, value);
            document.form_preferences.description_style[0].checked = true;
            document.form_preferences.description_style[1].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_display_style() {
    var fname = "ducc_preferences_display_style";
    try {
        var key = ducc_appl("display_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "textual") {
            document.form_preferences.display_style[0].checked = true;
            document.form_preferences.display_style[1].checked = false;
        } else if (value == "visual") {
            document.form_preferences.display_style[0].checked = false;
            document.form_preferences.display_style[1].checked = true;
        } else {
            value = "textual";
            ducc_put_cookie(key, value);
            document.form_preferences.display_style[0].checked = true;
            document.form_preferences.display_style[1].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_filter_users_style() {
    var fname = "ducc_preferences_filter_users_style";
    try {
        var key = ducc_appl("filter_users_style");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "include") {
            document.form_preferences.filter_users_style[0].checked = true;
            document.form_preferences.filter_users_style[1].checked = false;
            document.form_preferences.filter_users_style[2].checked = false;
            document.form_preferences.filter_users_style[3].checked = false;
        } else if (value == "include+active") {
            document.form_preferences.filter_users_style[0].checked = false;
            document.form_preferences.filter_users_style[1].checked = true;
            document.form_preferences.filter_users_style[2].checked = false;
            document.form_preferences.filter_users_style[3].checked = false;
        } else if (value == "exclude") {
            document.form_preferences.filter_users_style[0].checked = false;
            document.form_preferences.filter_users_style[1].checked = false;
            document.form_preferences.filter_users_style[2].checked = true;
            document.form_preferences.filter_users_style[3].checked = false;
        } else if (value == "exclude+active") {
            document.form_preferences.filter_users_style[0].checked = false;
            document.form_preferences.filter_users_style[1].checked = false;
            document.form_preferences.filter_users_style[2].checked = false;
            document.form_preferences.filter_users_style[3].checked = true;
        } else {
            value = "include";
            ducc_put_cookie(key, value);
            document.form_preferences.filter_users_style[0].checked = true;
            document.form_preferences.filter_users_style[1].checked = false;
            document.form_preferences.filter_users_style[2].checked = false;
            document.form_preferences.filter_users_style[3].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences_role() {
    var fname = "ducc_preferences_role";    
    try {
        var key = ducc_appl("role");
        var value = ducc_get_cookie(key);
        //alert("ducc_preferences"+" "+"key:"+key+" "+"value:"+value);
        if (value == "user") {
            document.form_preferences.role[0].checked = true;
            document.form_preferences.role[1].checked = false;
        } else if (value == "administrator") {
            document.form_preferences.role[0].checked = false;
            document.form_preferences.role[1].checked = true;
        } else {
            value = "user";
            ducc_put_cookie(key, value);
            document.form_preferences.role[1].checked = true;
            document.form_preferences.role[0].checked = false;
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_preferences() {
    var fname = "ducc_preferences";    
    try {
        ducc_preferences_table_style();
        ducc_preferences_date_style();
        ducc_preferences_description_style();
        ducc_preferences_display_style();
        ducc_preferences_filter_users_style();
        ducc_preferences_role();
    } catch (err) {
        ducc_error(fname, err);
    }
}
