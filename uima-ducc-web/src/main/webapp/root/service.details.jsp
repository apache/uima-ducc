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
  <script src="opensources/jquery-1.4.2.js" type="text/javascript"></script>
  <script src="opensources/jgrowl/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.8.4/gallery/jquery-ui-themes-1.8.4/themes/redmond/jquery-ui.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.8.4/ui/minified/jquery-ui.min.js"></script>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <script type="text/javascript" src="opensources/sorttable.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
  
  <script type="text/javascript">
	$(function() {
		$("#tabs").tabs();
	});
  </script>
  
</head>
<body onload="ducc_init('service-details');">

<table>
<tr>
<td>
<table class="heading">
<!-- *********************** row *************************** -->
<tr class="heading">
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
    <div>
    <ul id="accordion">
	  <li><a href="jobs.html">Jobs</a></li>
	  <ul></ul>
	  <li><a href="reservations.html">Reservations</a></li>
	  <ul></ul>
	  <li>Services</li>
	  <ul>
	  <li><a href="services.definitions.html">Definitions</a></li>
	  <li><a href="services.deployments.html">Deployments</a></li>
	  </ul>
	  <li>System</li>
	  <ul>
	  <li><a href="system.administration.html">Administration</a></li>
	  <li><a href="system.classes.html">Classes</a></li>
	  <li><a href="system.daemons.html">Daemons</a></li>
	  <li><a href="system.duccbook.html" target="_duccbook">DuccBook</a></li>
	  <li><a href="system.machines.html">Machines</a></li>
	  </ul>
	  <%@ include file="site.jsp" %>
    </ul>
    </div>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<div id="refreshbutton">
<input type="image" onclick="ducc_refresh('service-details');" title="Refresh" alt="Refresh" src="opensources/images/1284662827_refresh.png">
</div>
<div id="loading" style="display:none;">
<img title="loading" src="opensources/images/indicator.gif" style="border:1px solid #000000" alt="Loading...">
</div>
<br>
<table>
<tr>
<td align="left">
<form name="duccform">
<fieldset>
<legend>Refresh</legend>
<input type="radio" name="refresh" value="manual"            onclick="ducc_put_cookie('ducc:refreshmode','manual'   )" /> Manual
<br>
<input type="radio" name="refresh" value="automatic" checked onclick="ducc_put_cookie('ducc:refreshmode','automatic')" /> Automatic
</fieldset>
</form>
</table>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<h2>
<span class="idtitle" id="identity"></span>
</h2>
<form name="form_selectors">
<table>
<tr>
<td valign="top" align="right" title="The time of last Orchestrator publication">Updated:
<td valign="top"><span class="timestamptitle" id="timestamp_area"></span>
<tr>
<td valign="top" align="right">Authentication:
<td valign="top"><span class="authenticationtitle" id="authentication_area">?</span>
<tr>
<td valign="top" align="right">Max Records:
<td valign="top"><input type="text" size="8" id="maxrecs_input" value="default">
<tr>
<td valign="top" align="right">
<select id="users_select">
<option value="active+include">active+include</option>
<option value="active+exclude">active+exclude</option>
<option value="include">include</option>
<option value="exclude">exclude</option>
</select>
Users:
<td valign="top"><input type="text" size="16" id="users_input" value="default">
<tr>
<td>
<input type="hidden" id="users_select_input" value="default">
</table>
</form>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<h2><span class="subtitle">Service Deployment Details</span></h2>
<img src="opensources/images/90px-Mallard's_nest,_Culwatty_-_geograph.org.uk_-_813413.jpg" style="border:3px solid #ffff7a" alt="logo">
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<table>
<tr>
<td valign="middle" align="right">
<span id="login_link_area"></span>
 |
<span id="logout_link_area"><a href="logout.html" onclick="var newWin = window.open(this.href,'child','height=600,width=475,scrollbars'); newWin.focus(); return false;">Logout</a></span>
 |
<span id="duccbook_link_area"></span><a href="system.duccbook.html" target="_duccbook">DuccBook</a>
<tr>
<td valign="middle" align="center">
<div>
<br>
<h2><span class="title">ducc-mon</span></h2>
<span class="title_acronym">D</span><span class="title">istributed</span>
<span class="title_acronym">U</span><span class="title">IMA</span>
<span class="title_acronym">C</span><span class="title">luster</span>
<span class="title_acronym">C</span><span class="title">omputing</span>
<span class="title_acronym">Mon</span><span class="title">itor</span>
<br>
<i>Version: <span class="version" id="version"></span></i>
<br>
<br>
<table>
<tr>
<td valign="middle" align="left">
<small>Copyright &copy 2012 The Apache Software Foundation</small>
<tr>
<td valign="middle" align="left">
<small>Copyright &copy 2011, 2012 International Business Machines Corporation</small>
</table>
</div> 
</table>
<br>
</td>
</table>
<!-- *********************** row ************************ -->
<tr>
<td>
<table class="body">
<tr>
<td valign="middle" colspan="5">
&nbsp
<tr>
<td valign="middle" colspan="5">

		<div id="tabs"> 
		<ul>
			<li><a href="#tabs-1">Processes</a></li>
			<li><a href="#tabs-3">Specification</a></li>
		</ul>
			<div id="tabs-1">
				<table>
   	    			<caption><b>Processes List</b><br><i><small>click column heading to sort</small></i></caption>
   	   				<tr>
        			<td>
      	  				<table class="sortable">
						<thead>
						<tr class="ducc-head">
						<th title="The system assigned id for this process" class="sorttable_numeric">Id</th>
						<th title="The log file name associated with this process">Log</th>
						<th title="The host for this process">Host:name</th>
						<th title="The host IP for this process">Host:ip</th>
						<th title="The OS assigned PID for this process"class="sorttable_numeric">PID</th>
						<th title="Process scheduling state">State:scheduler</th>
						<th title="Process scheduling reason (for scheduling state)">Reason:scheduler</th>
						<th title="Process agent state">State:agent</th>
						<th title="Process agent reason (for agent state)">Reason:agent</th>
						<th title="Process initialization time">Time:initialization</th>
						<th title="Process total time">Time:total</th>
						<th title="The JConsole URL for this process">JConsole: URL</th>
						</tr>
						</thead>
						<tbody id="processes_list_area">
   	  					</tbody>
		 		 		</table>
   	    		</table>
			</div>
			<div id="tabs-3">
   				<div class="specification_data_div">
   					<span id="specification_data_area"></span>
   				</div>
			</div>
		</div>
</td>
</table>
</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
