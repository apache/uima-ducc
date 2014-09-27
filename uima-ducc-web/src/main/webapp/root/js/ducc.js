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

function ducc_identity() {
    var fname = "ducc_identity";
    var data = null;
    try {
        var servlet = "/ducc-servlet/cluster-name";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#identity").html(data);
            try {
                var a1 = data.split(">");
                var n1 = a1[1];
                var a2 = n1.split("<");
                var n2 = a2[0];
                var name = n2;
                $(document).attr("title", "ducc-mon: " + name);
                ducc_console_success(fname);
            } catch (err) {
                var message = fname + ".error: " + err;
                ducc_console_warn(message);
            }
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_version() {
    var fname = "ducc_version";
    var data = null;
    try {
        var servlet = "/ducc-servlet/version";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#version").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_password_checked() {
    var fname = "ducc_password_checked";
    var data = null;
    try {
        var servlet = "/ducc-servlet/authenticator-password-checked";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#password_checked_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_authenticator_version() {
    var fname = "ducc_authenticator_version";
    var data = null;
    try {
        var servlet = "/ducc-servlet/authenticator-version";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#authenticator_version_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_link_login() {
    var fname = "ducc_link_login";
    var data = null;
    try {
        var servlet = "/ducc-servlet/login-link";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#login_link_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_link_logout() {
    var fname = "ducc_link_logout";
    var data = null;
    try {
        var servlet = "/ducc-servlet/logout-link";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#logout_link_area").html(data);
           ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_timestamp() {
    var fname = "ducc_timestamp";
    var data = null;
    try {
        var servlet = "/ducc-servlet/timestamp";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#timestamp_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_authentication() {
    var fname = "ducc_authentication";
    var data = null;
    try {
        var servlet = "/ducc-servlet/user-authentication-status";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#authentication_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_utilization() {
    var fname = "ducc_utilization";
    var data = null;
    try {
        var servlet = "/ducc-servlet/cluster-utilization";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#utilization_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_viz_data() {
    var fname = "ducc_load_viz_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_viz_data + ms_reload_min) {
        return;
    }
    ms_load_viz_data = ms_now;
    try {
        var servlet = "/ducc-servlet/viz-nodes";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#viz-nodes").html(data);
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_classic_jobs_data() {
    var fname = "ducc_load_classic_jobs_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-jobs-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#jobs_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_classic_services_data() {
    var fname = "ducc_load_classic_services_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-services-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#services_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_service_summary_data() {
    var fname = "ducc_load_service_summary_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_summary_data + ms_reload_min) {
        return;
    }
    ms_load_service_summary_data = ms_now;
    try {
        var servlet = "/ducc-servlet/service-summary-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#service_summary_area").html(data);
            hide_show();
           ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_broker_summary_data() {
    var fname = "ducc_load_broker_summary_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_broker_summary_data + ms_reload_min) {
        return;
    }
    ms_load_broker_summary_data = ms_now;
    try {
        var servlet = "/ducc-servlet/broker-summary-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#broker_summary_area").html(data);
            hide_show();
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_job_workitems_count_data() {
    var fname = "ducc_load_job_workitems_count_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_workitems_count_data + ms_reload_min) {
        return;
    }
    ms_load_job_workitems_count_data = ms_now;
    try {
        var servlet = "/ducc-servlet/job-workitems-count-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#job_workitems_count_area").html(data);
            hide_show();
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_job_workitems_data() {
    var fname = "ducc_load_job_workitems_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_workitems_data + ms_reload_min) {
        return;
    }
    ms_load_job_workitems_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_workitems_area").html(data);
        var servlet = "/ducc-servlet/job-workitems-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#workitems_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_workitems_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_workitems_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_job_performance_data() {
    var fname = "ducc_load_job_performance_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_performance_data + ms_reload_min) {
        return;
    }
    ms_load_job_performance_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_performance_area").html(data);
        var servlet = "/ducc-servlet/job-performance-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#performance_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_performance_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_performance_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_job_specification_data() {
    var fname = "ducc_load_job_specification_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_specification_data + ms_reload_min) {
        return;
    }
    ms_load_job_specification_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_specification_area").html(data);
        var servlet = "/ducc-servlet/job-specification-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#specification_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_specification_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('specification_table'));
            }
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_specification_area").html(data);
            ducc_console_fail(fname, textStatus);
        });
    } catch (err) {
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

function ducc_load_job_files_data() {
    var fname = "ducc_load_job_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_files_data + ms_reload_min) {
        return;
    }
    ms_load_job_files_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        var servlet = "/ducc-servlet/job-files-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('files_table'));
            }
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });        
    } catch (err) {
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

function ducc_load_reservation_specification_data() {
    var fname = "ducc_load_reservation_specification_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_specification_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_specification_data = ms_now;
    try {
        var servlet = "/ducc-servlet/reservation-specification-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#specification_data_area").html(data);
            hide_show();
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('specification_table'));
            }
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });           
    } catch (err) {
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

function ducc_load_reservation_files_data() {
    var fname = "ducc_load_reservation_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_files_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_files_data = ms_now;
    try {
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        var servlet = "/ducc-servlet/reservation-files-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            var table_style = ducc_preferences_get("table_style");
            if (table_style == "scroll") {
                sorttable.makeSortable(document.getElementById('files_table'));
            }
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });        
    } catch (err) {
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

function ducc_load_service_registry_data() {
    var fname = "ducc_load_service_registry_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_registry_data + ms_reload_min) {
        return;
    }
    ms_load_service_registry_data = ms_now;
    try {
        var servlet = "/ducc-servlet/service-registry-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#registry_data_area").html(data);
            hide_show();
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });            
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_service_update_form_button = +new Date() - ms_reload_min;

function ducc_service_update_form_button() {
    var fname = "ducc_service_update_form_button";
    var data = null;
        var ms_now = +new Date();
    if (ms_now < ms_service_update_form_button + ms_reload_min) {
        return;
    }
    ms_service_update_form_button = ms_now;
    try {
        var servlet = "/ducc-servlet/service-update-get-form-button" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#service_update_form_button").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });            
    } catch (err) {
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

function ducc_load_service_deployments_data() {
    var fname = "ducc_load_service_deployments_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_deployments_data + ms_reload_min) {
        return;
    }
    ms_load_service_deployments_data = ms_now;
    try {
        var servlet = "/ducc-servlet/service-deployments-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#deployments_list_area").html(data);
            ducc_cluetips();
            hide_show();
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
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

function ducc_load_service_files_data() {
    var fname = "ducc_load_service_files_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_files_data + ms_reload_min) {
        return;
    }
    ms_load_service_files_data = ms_now;
    try {
        var servlet = "/ducc-servlet/service-files-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_files_area").html(data);
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#files_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_files_area").html(data);
            //var table_style = ducc_preferences_get("table_style");
            //if (table_style == "scroll") {
            //    sorttable.makeSortable(document.getElementById('files_table'));
            //}
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_files_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
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

function ducc_load_service_history_data() {
    var fname = "ducc_load_service_history_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_service_history_data + ms_reload_min) {
        return;
    }
    ms_load_service_history_data = ms_now;
    try {
        var servlet = "/ducc-servlet/service-history-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_history_area").html(data);
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#history_data_area").html(data);
            hide_show();
            data = "";
            $("#loading_history_area").html(data);
            //var table_style = ducc_preferences_get("table_style");
            //if (table_style == "scroll") {
            //    sorttable.makeSortable(document.getElementById('history_table'));
            //}
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#history_data_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                
    } catch (err) {
        data = "";
        $("#loading_history_area").html(data);
        ducc_error(fname, err);
    }
}

function hide_show() {
    var fname = "hide_show";
    try {
        var classpathdata = ducc_appl("classpathdata");
        var c_value = ducc_get_cookie(classpathdata);
        if (c_value == null) {
            c_value = "hide";
        }
        if (c_value == "hide") {
            $('div.showdata').hide();
            $('div.hidedata').show();
        }
        if (c_value == "show") {
            $('div.showdata').show();
            $('div.hidedata').hide();
        }
        $('#showbutton0').click(function() {
            $('div.showdata').show();
            $('div.hidedata').hide();
            ducc_put_cookie(classpathdata, "show")
        });
        $('#hidebutton0').click(function() {
            $('div.showdata').hide();
            $('div.hidedata').show();
            ducc_put_cookie(classpathdata, "hide")
        });
        $('#showbutton1').click(function() {
            $('div.showdata').show();
            $('div.hidedata').hide();
            ducc_put_cookie(classpathdata, "show")
        });
        $('#hidebutton1').click(function() {
            $('div.showdata').hide();
            $('div.hidedata').show();
            ducc_put_cookie(classpathdata, "hide")
        });
    } catch (err) {
        ducc_error(fname, err);
    }
}

var ms_load_job_processes_data = +new Date() - ms_reload_min;

function ducc_load_job_processes_data() {
    var fname = "ducc_load_job_processes_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_job_processes_data + ms_reload_min) {
        return;
    }
    ms_load_job_processes_data = ms_now;
    try {
        var servlet = "/ducc-servlet/job-processes-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_processes_area").html(data); 
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#processes_list_area").html(data);
            ducc_cluetips();
            hide_show();
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                    
    } catch (err) {
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

function ducc_load_reservation_processes_data() {
    var fname = "ducc_load_reservation_processes_data";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_processes_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_processes_data = ms_now;
    try {
        var servlet = "/ducc-servlet/reservation-processes-data" + location.search;
        var tomsecs = ms_timeout;
        data = "<img src=\"opensources/images/indicator.gif\" alt=\"waiting...\">";
        $("#loading_processes_area").html(data); 
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#processes_list_area").html(data);
            ducc_cluetips();
            hide_show();
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
        }).fail(function(jqXHR, textStatus) {
            data = "";
            $("#loading_processes_area").html(data);
            ducc_console_fail(fname, textStatus);
        });                  
    } catch (err) {
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

function ducc_load_classic_machines_data() {
    var fname = "ducc_load_classic_machines_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-system-machines-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#machines_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                      
    } catch (err) {
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

function ducc_reservation_form_button() {
    var fname = "ducc_reservation_form_button";
    var data = null;
    try {
        var servlet = "/ducc-servlet/reservation-get-form-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#reservation_form_button").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                      
    } catch (err) {
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

function ducc_load_classic_reservations_data() {
    var fname = "ducc_load_classic_reservations_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-reservations-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#reservations_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                 
    } catch (err) {
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

function ducc_load_reservation_scheduling_classes() {
    var fname = "ducc_load_reservation_scheduling_classes";
    var data = null;
    var ms_now = +new Date();
    if (ms_now < ms_load_reservation_scheduling_classes_data + ms_reload_min) {
        return;
    }
    ms_load_reservation_scheduling_classes_data = ms_now;
    try {
        var servlet = "/ducc-servlet/reservation-scheduling-classes";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#scheduling_class_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_reservation_instance_memory_sizes() {
    var fname = "ducc_load_reservation_instance_memory_sizes";
    var data = null;
    try {
        var servlet = "/ducc-servlet/reservation-instance-memory-sizes";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#instance_memory_sizes_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_reservation_instance_memory_units() {
    var fname = "ducc_load_reservation_instance_memory_units";
    var data = null;
    try {
        var servlet = "/ducc-servlet/reservation-instance-memory-units";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#instance_memory_units_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_reservation_number_of_instances() {
    var fname = "ducc_load_reservation_number_of_instances";
    var data = null;
    try {
        var servlet = "/ducc-servlet/reservation-number-of-instances";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#number_of_instances_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_reservation_submit_button() {
    var fname = "ducc_load_reservation_submit_button";
    var data = null;
    try {
        var servlet = "/ducc-servlet/reservation-get-submit-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#reservation_submit_button_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_submit_reservation_data() {
    var fname = "ducc_load_submit_reservation_data";
    try {
        ducc_load_reservation_scheduling_classes();
        ducc_load_reservation_instance_memory_sizes();
        ducc_load_reservation_instance_memory_units();
        ducc_load_reservation_number_of_instances();
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

function ducc_load_job_form() {
    var fname = "ducc_load_job_form";
    var data = null;
    try {
        var servlet = "/ducc-servlet/job-submit-form";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#job_submit_form_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_job_submit_button() {
    var fname = "ducc_load_job_submit_button";
    var data = null;
    try {
        var servlet = "/ducc-servlet/job-get-submit-button";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#job_submit_button_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_system_admin_admin_data() {
    var fname = "ducc_load_system_admin_admin_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/system-admin-admin-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#system_administration_administrators_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_load_system_admin_control_data() {
    var fname = "ducc_load_system_admin_control_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/system-admin-control-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#system_administration_control_area").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
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

function ducc_load_classic_system_classes_data() {
    var fname = "ducc_load_classic_system_classes_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-system-classes-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#system_classes_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
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

function ducc_load_classic_system_daemons_data() {
    var fname = "ducc_load_classic_system_daemons_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-system-daemons-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#system_daemons_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
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

function ducc_load_classic_system_broker_data() {
    var fname = "ducc_load_classic_system_broker_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/classic-system-broker-data";
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#system_broker_list_area").html(data);
            ducc_console_success(fname);
            ducc_load_common();
            ducc_cluetips();
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
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

function ducc_cookies() {
    var fname = "ducc_cookies";
    try {
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

function uima_initialization_report_summary() {
    var fname = "uima_initialization_report_summary";
    var data = null;
    try {
        var servlet = "/ducc-servlet/uima-initialization-report-summary" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#uima_initialization_report_summary").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
        ducc_error(fname, err);
    }
}

function uima_initialization_report_data() {
    var fname = "uima_initialization_report_data";
    var data = null;
    try {
        var servlet = "/ducc-servlet/uima-initialization-report-data" + location.search;
        var tomsecs = ms_timeout;
        $.ajax({
            url: servlet,
            timeout: tomsecs
        }).done(function(data) {
            $("#uima_initialization_report_data").html(data);
            ducc_console_success(fname);
        }).fail(function(jqXHR, textStatus) {
            ducc_console_fail(fname, textStatus);
        });                     
    } catch (err) {
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
        var appl = "ducc:";
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
        for (var i = 0; i < document.duccform.refresh.length; i++) {
            if (type == "jobs") {
                ducc_load_jobs_data();
            }
            if (type == "services") {
                ducc_load_services_data();
            }
            if (type == "reservations") {
                ducc_load_reservations_data();
            }
            if (type == "job-details") {
                ducc_load_job_workitems_count_data();
                ducc_load_job_processes_data();
                ducc_load_job_workitems_data();
                ducc_load_job_performance_data();
                //ducc_load_job_specification_data();
                ducc_load_job_files_data();
            }
            if (type == "reservation-details") {
                //ducc_load_reservation_specification_data();
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

var to_timed_loop = null;

function ducc_timed_loop(type) {
    var fname = "ducc_timed_loop";
    ducc_console_enter(fname);
    try {
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

function ducc_release_shares(node, type) {
    var fname = "ducc_release_shares";
    try {
        $.jGrowl(" Pending release...");
        $.ajax({
            type: 'POST',
            url: "/ducc-servlet/release-shares-request" + "?node=" + node + "&" + "type=" + type,
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

function ducc_confirm_release_shares(node, type) {
    var fname = "ducc_confirm_release_shares";
    try {
        var machine = node;
        if (machine == "*") {
            machine = "ALL machines"
        }
        var result = confirm("Release " + type + " shares on " + machine + "?");
        if (result == true) {
            ducc_release_shares(node, type);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
}

function ducc_confirm_terminate_job(id) {
    var fname = "ducc_confirm_release_shares";
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
    var fname = "ducc_confirm_terminate_service";
    try {
        var result = confirm("Terminate service " + id + "?");
        if (result == true) {
            ducc_terminate_service(id);
        }
    } catch (err) {
        ducc_error(fname, err);
    }
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

            $.ajax({
                type: 'POST',
                async: false,
                url: "/ducc-servlet/reservation-submit-request",
                //data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'instance_memory_units':instance_memory_units,'number_of_instances':number_of_instances,'description':description},
                data: {
                    'scheduling_class': scheduling_class,
                    'instance_memory_size': instance_memory_size,
                    'number_of_instances': number_of_instances,
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
                //data: {'scheduling_class':scheduling_class,'instance_memory_size':instance_memory_size,'instance_memory_units':instance_memory_units,'number_of_instances':number_of_instances,'description':description},
                data: {
                    'scheduling_class': scheduling_class,
                    'instance_memory_size': instance_memory_size,
                    'number_of_instances': number_of_instances,
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