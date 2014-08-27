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
  <script type="text/javascript">
	$(function() {
		$("#tabs").tabs();
	});
  </script>
<%
if (table_style.equals("scroll")) {
%>  
  <script type="text/javascript" charset="utf-8">
	var oTable;
	$(document).ready(function() {
		oTable = $('#services-table').dataTable( {
		 dom: 'C<"clear">lfrtip',		
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-services",
			"aaSorting": [],
			"aoColumnDefs": [ { "bSortable": false, "aTargets": [ 0, 1 ] } ],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
                             		$('td:eq(2)', nRow).css( "text-align", "right" );
                             		$('td:eq(6)', nRow).css( "text-align", "right" );
                             		$('td:eq(7)', nRow).css( "text-align", "right" );
                             		$('td:eq(11)', nRow).css( "text-align", "right" );
                             		$('td:eq(12)', nRow).css( "text-align", "right" );
                             		$('td:eq(13)', nRow).css( "text-align", "right" );
                             		$('td:eq(14)', nRow).css( "text-align", "right" );
                             		$('td:eq(15)', nRow).css( "text-align", "right" );
                             		$('td:eq(16)', nRow).css( "text-align", "right" );
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
</head>
<body onload="ducc_init('services');" onResize="ducc_resize();">

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
<%@ include file="$banner/c1-refresh-services.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-services.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-services.jsp" %>
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
	<table width="100%">
	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Services Definitions List</b><br><i><small>click column heading to sort</small></i></caption>
	</table>
	<table id="services-table" width="100%">
	<thead>
	<tr class="ducc-header">
		<th class="ducc-col-button"></th>
		<th class="ducc-col-button"></th>
		<th title="The service Id">Id</th>
		<th title="The service name">Name</th>
		<th title="The service state">State</th>
		<th title="The time this service was last used">Last Use</th>
		<th title="The service number of instances registered">Instances</th>
		<th title="The service number of processes currently started">Deployments</th>
		<th title="The service start mode">Start<br>Mode</th>
		<th class="ducc-no-filter" id="user_column_heading" title="The service owning user">User</th>
		<th title="The service scheduling class">Class</th>
		<th title="The count for all service processes of major faults which required loading a memory page from disk">Pgin</th>
		<th title="The GB swapped out to disk for all initializing/running service processes">Swap</th>
		<th title="The service process memory size (GB)">Size</th>
		<th title="The number of active Jobs that depend on this service">Jobs</th>
		<th title="The number of active Services that depend on this service">Ser-<br>vices</th>
		<th title="The number of active Reservations that depend on this service">Reser-<br>vations</th>
		<th title="The service description">Description</th>
	</tr>
	</thead>
	<tbody id="services_list_area">
	</tbody>
	</table>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
	<table width="100%">
   	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Services Definitions List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th class="ducc-col-button"></th>
		<th class="ducc-col-button"></th>
		<th class="sorttable_numeric" title="The service Id">Id</th>
		<th title="The service name">Name</th>
		<th title="The service state">State</th>
		<th title="The time this service was last used">Last Use</th>
        <th title="The service number of instances registered">Instances</th>
        <th title="The service number of processes currently started">Deployments</th>
		<th title="The service start mode">Start<br>Mode</th>
		<th class="ducc-no-filter" id="user_column_heading" title="The service owning user">User</th>
		<th title="The service scheduling class">Class</th>
		<th class="sorttable_numeric" title="The count for all service processes of major faults which required loading a memory page from disk">Pgin</th>
		<th class="sorttable_numeric" title="The GB swapped out to disk for all initializing/running service processes">Swap</th>
		<th class="sorttable_numeric" title="The service process memory size (GB)">Size</th>
		<th class="sorttable_numeric" title="The number of active Jobs that depend on this service">Jobs</th>
		<th class="sorttable_numeric" title="The number of active Services that depend on this service">Ser-<br>vices</th>
		<th class="sorttable_numeric" title="The number of active Reservations that depend on this service">Reser-<br>vations</th>
		<th title="The service description">Description</th>
		</tr>
		</thead>
		<tbody id="services_list_area">
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
