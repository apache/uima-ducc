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

<body onload="ducc_init('submit-reservation');">
<table>

<tr>
<td valign="top">
  <div>
    <span>
      <h2><span class="title">ducc-mon</span></h2>
      <h4><span class="fulltitle">Distributed</span>
          <br>
          <span class="fulltitle">UIMA</span>
          <br>
          <span class="fulltitle">Cluster</span>
          <br>
          <span class="fulltitle">Computing</span>
          <br>
          <span class="fulltitle">Monitor</span>
          <br>
      	  <br>
      	  version: <span class="version" id="version"></span>
      	  <br>
      	  <br>
      	  <span class="idtitle" id="identity"></span>
      </h4>
      <h3><span class="subtitle">Reservation<span></h3>
      <h5><span class="timestamptitle">updated: </span><span class="timestamptitle" id="timestamp_area"></span>
          <br>
          <span class="authenticationtitle">authentication status: </span><span class="authenticationtitle" id="authentication_area"></span>
      </h5>
    </span>
    <span>
      <img src="opensources/images/2x1.33in-Bariken_kid.JPG" style="border:3px solid #ffff7a" alt="logo">
    </span>
    <br>
    
<td valign="top"> 
  <div>
    <span>
      <h2><span class="title">&nbsp </span></h2>
    </span>
    <span class="noshow">
    <h3>Refresh</h3>
    <table>
    <tr>
    <td>
      <div id="refreshbutton">
        <input type="image" onClick="ducc_refresh('submit-reservation')" title="Refresh" alt="Refresh" src="opensources/images/refresh.png">
      </div>
    <td>
      <div>
        <form name="duccform">
		<input type="radio" name="refresh" value="manual"            onClick="ducc_put_cookie('ducc:refreshmode','manual'   )" /> Manual
		<input type="radio" name="refresh" value="automatic" checked onClick="ducc_put_cookie('ducc:refreshmode','automatic')" /> Automatic
		</form> 
      </div>
    </table>
    </span>
    
  </div>
  
  	<div class="segment">
      <div class="logout">
        <h3>Reservation</h3>
        <table>
        <tr>
        <td align="right">description
        <td align="right">
        <td align="left" ><span id="description_area"><input type="text" size="40" id="description">
                          </span>
        <tr>
        <td align="right">scheduling class
        <td align="right">
        <td align="left" ><span id="scheduling_class_area">
                          </span>
        <tr>
        <td align="right">instance memory size
        <td align="right">
        <td align="left" ><span id="instance_memory_sizes_area">
                          </span>
        <tr>
        <td align="right">instance memory units
        <td align="right">
        <td align="left" ><span id="instance_memory_units_area">
                          </span>
        <tr>
        <td align="right">number of instances
        <td align="right">
        <td align="left" ><span id="number_of_instances_area">
                          </span>
        <tr>
        <td align="right">wait for result
        <td align="right">
        <td align="left" ><span id="wait_for_result_area">
                          <table>
                          <tr>
                          <td align="left"><input type="radio"  id="wait_for_result_yes" name="wait_for_result" value="yes" checked /> Yes
                          <td align="left"><input type="radio"  id="wait_for_result_no"  name="wait_for_result" value="no" /> No
                          </table>
                          </span>
        <tr>
        <td align="right">
        <td align="right">
        <td align="left">
        <table>
        <tr>
        <td align="left" ><span id="reservation_submit_button_area">
                          </span>
        <td align="left" ><input type="button" onclick="ducc_cancel_submit_reservation()" value="Cancel" />
        </table>
        <tr>
        <td align="right"><span id="working_area" style="display:none;">
        					<input type="image" title="Working..." alt="Working..." src="opensources/images/indicator.gif">
                          </span>
        <td align="right">
        <td align="left" >
        </table>
   	  </div>
    </div>
    
  </div>

<tr>
<td>  
  <table>
	<tr>
	<td valign="middle" align="left">
	<%@ include file="$copyright.jsp" %>	
	</table>
<td>

</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
