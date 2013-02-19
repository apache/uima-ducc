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
<%
String table_style = "scroll";
String cookieName = "ducc:table_style";
String cookieValue = null;
Cookie cookie = null;
Cookie cookies [] = request.getCookies ();
if (cookies != null)
{
  for (int i = 0; i < cookies.length; i++) 
  {
    if (cookies [i].getName().equals (cookieName))
    {
      cookie = cookies[i];
      cookieValue = cookie.getValue();
      if(cookieValue != null) {
        table_style = cookieValue;
      }
      break;
    }
  }
}
%>
<html>
<head>
  <link rel="shortcut icon" href="uima.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <script src="opensources/jquery-1.4.2.js" type="text/javascript"></script>
  <script src="opensources/cluetip/jquery.cluetip.js" type="text/javascript"></script>
  <link href="opensources/cluetip/jquery.cluetip.css" rel="stylesheet" type="text/css">
<%
if (table_style.equals("scroll")) {
%>
  <script type="text/javascript" language="javascript" src="opensources/DataTables-1.9.1/media/js/jquery.dataTables.min.js"></script>
  <script type="text/javascript" language="javascript" src="opensources/DataTables-plugins/fnReloadAjax.js"></script>
<%
}
%>
  <script src="opensources/jgrowl/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.8.4/gallery/jquery-ui-themes-1.8.4/themes/redmond/jquery-ui.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.8.4/ui/minified/jquery-ui.min.js"></script>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
<%
if (table_style.equals("scroll")) {
%>  
  <script type="text/javascript" charset="utf-8">
	var oTable;
	$(document).ready(function() {
		oTable = $('#jobs-table').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-jobs",
			"aaSorting": [],
			"aoColumnDefs": [ { "bSortable": false, "aTargets": [ 0 ] } ],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
									$('td:eq(0)', nRow).css( "text-align", "right" );
                             		$('td:eq(1)', nRow).css( "text-align", "right" );
                             		$('td:eq(8)', nRow).css( "text-align", "right" );
                             		$('td:eq(9)', nRow).css( "text-align", "right" );
                             		$('td:eq(10)', nRow).css( "text-align", "right" );
                             		$('td:eq(11)', nRow).css( "text-align", "right" );
                             		$('td:eq(12)', nRow).css( "text-align", "right" );
                             		$('td:eq(13)', nRow).css( "text-align", "right" );
                             		$('td:eq(14)', nRow).css( "text-align", "right" );
                             		$('td:eq(15)', nRow).css( "text-align", "right" );
                             		$('td:eq(16)', nRow).css( "text-align", "right" );
                             		$('td:eq(17)', nRow).css( "text-align", "right" );
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
<script type="text/javascript" src="opensources/sorttable.js"></script>
<%
}
%>
</head>
<body onload="ducc_init('jobs');" onResize="window.location.href = window.location.href;">

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
<%@ include file="$banner/c1-refresh-jobs.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-jobs.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-jobs.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c4-ducc-mon.jsp" %>
</td>
</table>
<!-- *********************** /table ************************ -->
<!-- ####################### /common ####################### -->
<!-- @@@@@@@@@@@@@@@@@@@@@@@ unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
<%
if (table_style.equals("scroll")) {
%>
<!--
	<table id="jobs-table" width="100%">
   	    <caption><b>Jobs List</b><br><i><small>click column heading to sort</small></i></caption>
			<thead>
			<tr class="ducc-header">
			<th class="ducc-col-terminate"></th>
			<th title="The system assigned id for this job" class="sorttable_numeric">Id</th>
			<th title="The time this job was submitted">Start</th>
			<th title="The time this job was finished (or the projected time until finish if presently running)">End</th>
			<th class="ducc-no-filter" id="user_column_heading" title="The user who submitted this job">User</th>
			<th title="The user specified class of this job">Class</th>
			<th title="The current state of this job">State</th>
			<th title="The reason for the final state of this job, normally EndOfJob; or extraordinary runtime status">Reason<br><small>or extraordinary status</small></th>
			<th title="The number of job processes currently running">Proc-<br>esses</th>
			<th title="The number of job processes that failed during initialization">Init<br>Fails</th>
			<th title="The number of job processes that failed during runtime (post-initialization)">Run<br>Fails</th>
			<th title="Each job process size, in GB">Size</th>
			
			<th title="The total number of work items for this job">Total</th>
			<th title="The number of work items that completed successfully">Done</th>
			<th title="The number of work items that failed to complete successfully">Error</th>
			<th title="The number of work items currently dispatched (queued+operating)">Dis-<br>patch</th>
			<th title="The number of work items that were retried, excluding preemptions">Retry</th>
			<th title="Tne number of work items that were preempted">Pre-<br>empt</th>
			<th title="The user specified description of this job">Description</th>
			</tr>
			</thead>
			<tbody id="jobs_list_area">
   	  		</tbody>
   	</table>
-->   	
	<table id="jobs-table" width="100%">
	<caption><b>Jobs List</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
	<th class="ducc-col-terminate"></th>
	<th title="The system assigned id for this job" class="sorttable_numeric">Id</th>
	<th title="The time this job was submitted">Start</th>
	<th title="The time this job was finished (or the projected time until finish if presently running)">End</th>
	<th class="ducc-no-filter" id="user_column_heading" title="The user who submitted this job">User</th>
	<th title="The user specified class of this job">Class</th>
	<th title="The current state of this job">State</th>
	<th title="The reason for the final state of this job, normally EndOfJob; or extraordinary runtime status">Reason<br><small>or extraordinary status</small></th>
	<th title="The number of job processes currently running">Proc-<br>esses</th>
	<th title="The number of job processes that failed during initialization">Init<br>Fails</th>
	<th title="The number of job processes that failed during runtime (post-initialization)">Run<br>Fails</th>
	<th title="Each job process size, in GB">Size</th>
	<th title="The total number of work items for this job">Total</th>
	<th title="The number of work items that completed successfully">Done</th>
	<th title="The number of work items that failed to complete successfully">Error</th>
	<th title="The number of work items currently dispatched (queued+operating)">Dis-<br>patch</th>
	<th title="The number of work items that were retried, excluding preemptions">Retry</th>
	<th title="Tne number of work items that were preempted">Pre-<br>empt</th>
	<th title="The user specified description of this job">Description</th>
	</tr>
	</thead>
	<tbody id="jobs_list_area">
	</tbody>
	</table>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
	<table width="100%">
   	<caption><b>Jobs List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th class="ducc-col-terminate"></th>
		<th title="The system assigned id for this job" class="sorttable_numeric">Id</th>
		<th title="The time this job was submitted">Start</th>
		<th title="The time this job was finished (or the projected time until finish if presently running)">End</th>
		<th class="ducc-no-filter" id="user_column_heading" title="The user who submitted this job">User</th>
		<th title="The user specified class of this job">Class</th>
		<th title="The current state of this job">State</th>
		<th title="The reason for the final state of this job, normally EndOfJob; or extraordinary runtime status">Reason<br><small>or extraordinary status</small></th>
		<th title="The number of job processes currently running">Proc-<br>esses</th>
		<th title="The number of job processes that failed during initialization">Init<br>Fails</th>
		<th title="The number of job processes that failed during runtime (post-initialization)">Run<br>Fails</th>
		<th title="Each job process size, in GB">Size</th>
		<th title="The total number of work items for this job">Total</th>
		<th title="The number of work items that completed successfully">Done</th>
		<th title="The number of work items that failed to complete successfully">Error</th>
		<th title="The number of work items currently dispatched (queued+operating)">Dis-<br>patch</th>
		<th title="The number of work items that were retried, excluding preemptions">Retry</th>
		<th title="Tne number of work items that were preempted">Pre-<br>empt</th>
		<th title="The user specified description of this job">Description</th>
		</tr>
		</thead>
		<tbody id="jobs_list_area">
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
