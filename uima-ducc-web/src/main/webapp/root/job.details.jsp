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
  <link rel="shortcut icon" href="ducc.ico" />
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
<body onload="ducc_init('job-details');">

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
<input type="image" onClick="ducc_refresh('job-details')" title="Refresh" alt="Refresh" src="opensources/images/1284662827_refresh.png">
</div>
<br>
<table>
<tr>
<td align="left">
<form name="duccform">
<fieldset>
<legend>Refresh</legend>
<input type="radio" name="refresh" value="manual"    checked onClick="ducc_put_cookie('ducc_refresh_mode','manual'   )" /> Manual
<br>
<input type="radio" name="refresh" value="automatic"         onClick="ducc_put_cookie('ducc_refresh_mode','automatic')" /> Automatic
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
<td valign="top" align="right">Updated:
<td valign="top"><span class="timestamptitle" id="timestamp_area"></span>
<tr>
<td valign="top" align="right">Authentication:
<td valign="top"><span class="authenticationtitle" id="authentication_area">?</span>
<!--
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
-->
<tr>
<td>
<input type="hidden" id="users_select_input" value="default">
</table>
</form>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<h2><span class="subtitle">Job Details</span></h2>
<img src="opensources/images/120px-Anas_platyrhynchos_-United_Kingdom_-adult_female_and_ducklings-8.jpg" style="border:3px solid #ffff7a" alt="logo">
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
<span id="job_workitems_count_area"></span>
<tr>
<td valign="middle" colspan="5">
&nbsp
<tr>
<td valign="middle" colspan="5">

		<div id="tabs"> 
		<ul>
			<li><a href="#tabs-1">Processes</a></li>
			<li><a href="#tabs-2">Work Items</a></li>
			<li><a href="#tabs-3">Performance</a></li>
			<li><a href="#tabs-4">Specification</a></li>
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
						<th title="The log file size, in MB">Size</th>
						<th title="The host for this process">Host<br>Name</th>
						<!--
						<th title="The host IP for this process">Host<br>ip</th>
						-->
						<th title="The OS assigned PID for this process"class="sorttable_numeric">PID</th>
						<th title="Process scheduling state">State<br>Scheduler</th>
						<th title="Process scheduling reason (for scheduling state)">Reason<br>Scheduler<br><small>or extraordinary status</small></th>
						<th title="Process agent state">State<br>Agent</th>
						<th title="Process agent reason (for agent state)">Reason<br>Agent</th>
						<th title="Process initialization time, hh:mm:ss">Time<br>Init</th>
						<th title="Process run time (not including process initialization time), hh:mm:ss">Time<br>Run</th>
						<th title="Process total time spent performing garbage collections, hh:mm:ss">Time<br>GC</th>
						<th title="Process total number of garbage collections that have occurred">Count<br>GC</th>
						<th title="Process percentage of time spent in garbage collections, relative to total of initialization + run times">%GC</th>
						<th title="Cumulative CPU time, hh:mm:ss">CPU</th>
						<th title="Resident Storage Size, as a percentage of process memory requirement in job specification">%RSS</th>
						<th title="Average seconds per work item">Time<br>Avg</th>
						<th title="Maximum seconds for any work item">Time<br>Max</th>
						<th title="Minimum seconds for any work item">Time<br>Min</th>
						<th title="The number of work items that completed successfully">Done</th>
						<th title="The number of work items that failed to complete successfully">Error</th>
						<th title="The number of work items that were retried, excluding preemptions">Retry</th>
						<th title="The number of work items that were preempted">Pre-<br>empt</th>
						<th title="The JConsole URL for this process">JConsole<br>URL</th>
						</tr>
						</thead>
						<tbody id="processes_list_area">
   	  					</tbody>
		 		 		</table>
   	    		</table>
			</div>
			<div id="tabs-2">
				<table>
   	    			<caption><b>Work Items List</b><br><i><small>click column heading to sort</small></i></caption>
   	    			<tr>
        			<td>
      	  			<table class="sortable">
						<thead>
						<tr class="ducc-head">
						<th title="The system assigned work item sequence number" class="sorttable_numeric">SeqNo</th>
						<th title="The user assigned work item identity">Id</th>
						<th title="The work item status, normally operating or ended">Status</th>
						<th title="The approx. number of seconds elapsed between work item (a) queued for processing and (b) dequeued for start of processing" class="sorttable_numeric">Queuing Time (sec)</th>
						<th title="The approx. number of seconds elapsed between work item (a) dequeued for start of processing and (b) end of processing" class="sorttable_numeric">Processing Time (sec)</th>
						<th title="The node for the work item, by address">Node (IP)</th>
						<th title="The node for the work item, by name">Node (Name)</th>
						<th title="The PID for the work item" class="sorttable_numeric">PID</th>
						</tr>
						</thead>
						<tbody id="workitems_data_area">
   	  					</tbody>
			  			</table>
   	    		</table>
			</div>
			<div id="tabs-3">
				<div class="performance_data_div">
   					<span id="performance_data_area"></span>
   				</div>
			</div>
			<div id="tabs-4">
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
