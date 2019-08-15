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
<%@ page language="java" %>
<html>
<head>
  <link rel="shortcut icon" href="uima.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <%@ include file="$imports.jsp" %>
<%
if (table_style.equals("scroll")) {
%>  
  <script type="text/javascript" charset="utf-8">
	var oTable;
	$(document).ready(function() {
		oTable = $('#experiment-details-table').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-experiment-details",
			"aaSorting": [],
			"aoColumnDefs": [ { "bSortable": false, "aTargets": [ 0 ] } ],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
</head>
<body onload="ducc_init('experiment-details');" onResize="ducc_resize();">

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
<%@ include file="$banner/$runmode.jsp" %>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c1-refresh-experiment-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-experiment-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-experiment-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c4-ducc-mon.jsp" %>
</td>
</table>
<!-- *********************** /table ************************ -->
<!-- ####################### /common ####################### -->
<div id=identify_experiment_details_area>
</div>
<!-- @@@@@@@@@@@@@@@@@@@@@@@ unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
<%
if (table_style.equals("scroll")) {
%>
	<div id=loading_experiment_details_area>
	</div>
	<table id="experiment-details-table" width="100%">
	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Experiment Details</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
	<th title="The id for this experiment subtask" class="sorttable_numeric">Id</th>
	<th title="The name for this experiment subtask">Name</th>
	<th title="The parent id for this experiment subtask" class="sorttable_numeric">Parent</th>
	<th title="The state of this experiment subtask">State</th>
	<th title="The type for this experiment subtask">Type</th>
	<th title="The start time of this experiment subtask">Step<br>Start</th>
	<th title="The duration time of this experiment subtask">Step<br>Duration</th>
	<th title="The DUCC Id(s) for this experiment subtask, if any">DUCC Id</th>
	<th title="The duration time of this DUCC subtask">DUCC<br>Duration</th>
	<th title="The total number of work items for this job">Total</th>
	<th title="The number of work items that completed successfully">Done</th>
	<th title="The number of work items that failed to complete successfully">Error</th>
	<th title="The number of work items currently dispatched (queued+operating)">Dis-<br>patch</th>
	<th title="The number of work items that were retried, excluding preemptions">Retry</th>
	<th title="The number of work items that were preempted">Pre-<br>empt</th>
	</tr>
	</thead>
	<tbody id="experiment_details_area">
	</tbody>
	</table>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
	<div id=loading_experiment_details_area>
	</div>
	<table width="100%">
   	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Experiment Details</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th title="The id for this experiment subtask" class="sorttable_numeric">Id</th>
        <th title="The name for this experiment subtask">Name</th>
        <th title="The parent id for this experiment subtask" class="sorttable_numeric">Parent</th>
        <th title="The state of this experiment subtask">State</th>
        <th title="The type for this experiment subtask">Type</th>
        <th title="The start time of this experiment subtask">Step<br>Start</th>
        <th class="sorttable_numeric" title="The duration time of this experiment subtask">Step<br>Duration</th>
		<th title="The DUCC Id(s) for this experiment subtask, if any">DUCC Id</th>
		 <th class="sorttable_numeric" title="The duration time of this DUCC subtask">DUCC<br>Duration</th>
		<th title="The total number of work items for this job">Total</th>
		<th title="The number of work items that completed successfully">Done</th>
		<th title="The number of work items that failed to complete successfully">Error</th>
		<th title="The number of work items currently dispatched (queued+operating)">Dis-<br>patch</th>
		<th title="The number of work items that were retried, excluding preemptions">Retry</th>
		<th title="The number of work items that were preempted">Pre-<br>empt</th>
		</tr>
		</thead>
		<tbody id="experiment_details_area">
   		</tbody>
	  </table>
   	</table>
<%
}
%>	    
<!-- @@@@@@@@@@@@@@@@@@@@@@@ /unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
<!-- ####################### common ######################### -->
</div>
		
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
