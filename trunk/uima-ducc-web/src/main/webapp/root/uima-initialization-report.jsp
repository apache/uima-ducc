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
<html>
<head>
  <link rel="shortcut icon" href="uima.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <script src="opensources/jquery-2.0.3.min.js" type="text/javascript"></script>
  <script src="opensources/jgrowl-1.3/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl-1.3/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.theme.min.css" rel="stylesheet" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.structure.min.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.11.0.custom/jquery-ui.min.js"></script>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <script type="text/javascript" src="opensources/sorttable.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
</head>

<body onload="ducc_init('uima-initialization-report');">
<table width="100%">

<tr>
<td>
  <div style="display:none">
    <span>
      <h2><span class="title">&nbsp </span></h2>
    </span>
    <h3>Refresh</h3>
    <table>
    <tr>
    <td>
      <div id="refreshbutton">
        <input type="image" onClick="location.reload()" title="Refresh" alt="Refresh" src="opensources/images/refresh.png">
      </div>
    <td>
      <div>
        <form name="duccform">
        <input type="radio" name="refresh" value="manual"            onClick="ducc_put_cookie('DUCCrefreshmode','manual'   )" /> Manual
        <input type="radio" name="refresh" value="automatic" checked onClick="ducc_put_cookie('DUCCrefreshmode','automatic')" /> Automatic
        </form> 
      </div>
    </table>
  </div>
  
<tr>
<td>  
  <span id="uima_initialization_report_summary">?</span> 
</td>

<tr>
<td>
  <!--
  <table class="sortable">
  -->
  <table>
  <caption><b>UIMA Process Initialization Details</b></caption>
  <thead>
  <tr class="ducc-head">
  <th>Name
  <th>State
  <th>Time<br><small>ddd:hh:mm:ss</small>
  </thead>
  <tbody id="uima_initialization_report_data">
  </tbody>
  </table>
</td>

<tr>
<td>  
  <%@ include file="$copyright.jsp" %>	
</td>

</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
