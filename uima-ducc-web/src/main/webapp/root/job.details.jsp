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
<body onload="ducc_init('job-details');">

<!-- ####################### common ######################## -->
<div class="flex-page">
<!-- *********************** table ************************* -->
<table class="flex-heading">
<!-- *********************** row *************************** -->
<tr class="heading">
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c0-menu.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c1-refresh-job-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-job-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-job-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c4-ducc-mon.jsp" %>
</td>
</table>
<!-- *********************** /table ************************ -->
</div>
<!-- ####################### /common ####################### -->

<table>
<!-- *********************** row ************************ -->
<tr>
<td>
<table class="body">
<tr>
<td valign="middle" colspan="5">
<span id="job_workitems_count_area"></span>
<!--
<tr>
<td valign="middle" colspan="5">
&nbsp
-->
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
			    <%@ include file="job.details.table.processes.jsp" %>
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
