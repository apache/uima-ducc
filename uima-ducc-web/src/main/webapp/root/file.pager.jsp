<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/html">
<head>
    <meta charset='utf-8'>
    <meta http-equiv="X-UA-Compatible" content="chrome=1">
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">

    <link href="ducc.css" rel="stylesheet">
    <link href="opensources/jPagination/css/bootstrap.min.css" rel="stylesheet">
    
    <script src="opensources/jquery-2.0.3.min.js" type="text/javascript"></script>
    <script src="opensources/jPagination/js/bootstrap/bootstrap.min.js"></script>
    <script src="opensources/jPagination/js/jquery.twbsPagination.js" type="text/javascript"></script>
    
    <%
    int count = 1;
    int start = count;
    int display = count;
    int displayMax = 10;
    if (request.getParameter("pages") == null) {
    } 
    else {
        String pages = request.getParameter("pages");
        count = Integer.parseInt(pages);
        start = count;
        display = count;
        if(display > displayMax) {
            display = displayMax;
        }
    }
    out.println("<script type=\"text/javascript\">");
    out.println("$(document).ready(function () {");
    out.println("  $('#pagination-demo').twbsPagination({");
    out.println("    startPage: "+count+",");
    out.println("    totalPages: "+count+",");
    out.println("    visiblePages: "+display+",");
    out.println("    onPageClick: function (event, page) {");
    out.println("       $('#page-content').text('Page ' + page);");
    out.println("       ducc_load_log_file(page)");
    out.println("    }");
    out.println("  });");
    out.println("});");
    out.println("</script>");
    %>

</head>

<body onload="ducc_init_log_file();">
  <form name="duccform" style="margin-bottom:0;display:none">
    <fieldset>
    <input type="radio" name="refresh" value="manual"  />
    <input type="radio" name="refresh" value="automatic" />
    </fieldset>
  </form>
    <div class="log-display-top">
        <b style="display:none">
        <small>
        <%
        if (request.getParameter("fname") == null) {
            out.println("?");
        } 
        else {
            out.println(request.getParameter("fname"));
        }
        %>
        </small>
        </b>
        <div class="text-left">
            <ul id="pagination-demo" class="pagination-sm"></ul>
        </div>
        <div class="content">
            <div id="logfile-pagination" class="demo">
                <div id="logfile">                   
                </div>
            </div>
        </div>
    </div>
    
    <div class="log-display-bot">
        <div style="white-space:pre-wrap;">
            <div id="log_file_page_area">
            </div>
        </div>
    </div>
  
  <script type="text/javascript">
  String.prototype.startsWith = function(prefix) {
      return this.indexOf(prefix) === 0;
  }
  String.prototype.endsWith = function(suffix) {
	  return this.indexOf(suffix, this.length - suffix.length) !== -1;
  }
  function ducc_init_log_file() {
    var queryDict = {}
    location.search.substr(1).split("&").forEach(function(item) {queryDict[item.split("=")[0]] = item.split("=")[1]})
    var fname = queryDict["fname"]
    var page = 0;
    var url = "/ducc-servlet/file-contents?fname="+fname+"&page="+page;
    try {
        $.ajax(
        {
            url : url,
            success : function (data) 
            {
                pre = "";
                post = "";
                if(data.startsWith("<pre>")) {
                        pre = "<pre>";
                        data = data.substring(5);
                }
                if(data.endsWith("</pre>\n")) {
                        post = "</pre>\n";
                        data = data.substring(0,data.length-7);
                }
                data = data.replace(/</g, "&lt"); 
                data = data.replace(/>/g, "&gt");
                if(data.length <= 0) {
                	data = "No data found.\n";
                }
                $("#log_file_page_area").html(pre+data+post);
            }
        });
    }
    catch(err) {
        ducc_error("ducc_init_log_file",err);
    }
  }
  function ducc_load_log_file(page) {
    var queryDict = {}
    location.search.substr(1).split("&").forEach(function(item) {queryDict[item.split("=")[0]] = item.split("=")[1]})
    var fname = queryDict["fname"]
    var url = "/ducc-servlet/file-contents?fname="+fname+"&page="+page;
    //alert(url);
    try {
        $.ajax(
        {
            url : url,
            success : function (data) 
            {
                pre = "";
                post = "";
                if(data.startsWith("<pre>")) {
                        pre = "<pre>";
                        data = data.substring(5);
                }
                if(data.endsWith("</pre>\n")) {
                        post = "</pre>\n";
                        data = data.substring(0,data.length-7);
                }
                data = data.replace(/</g, "&lt"); 
                data = data.replace(/>/g, "&gt");
                if(data.length <= 0) {
                	data = "No data found.\n";
                }
                $("#log_file_page_area").html(pre+data+post);
            }
        });
    }
    catch(err) {
        ducc_error("ducc_init_log_file",err);
    }
  }
  </script>

</body>
</html>
