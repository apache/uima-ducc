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
		oTable = $('#reservations-table').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-reservations",
			"aaSorting": [],
			"aoColumnDefs": [ { "bSortable": false, "aTargets": [ 0 ] } ],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
									$('td:eq(0)', nRow).css( "text-align", "right" );
                             		$('td:eq(1)', nRow).css( "text-align", "right" );
                             		$('td:eq(9)', nRow).css( "text-align", "right" );
                             		$('td:eq(10)', nRow).css( "text-align", "right" );
                             		$('td:eq(11)', nRow).css( "text-align", "right" );
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
</head>
<body onload="ducc_init('reservations');" onResize="window.location.href = window.location.href;">

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
<div id="reservation_form_button">
<button title="Login to enable" disabled style="font-size:8pt;">Request<br>Reservation</button>
</div>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c1-refresh-reservations.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-reservations.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-reservations.jsp" %>
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
	<table id="reservations-table" width="100%">
	<caption><b>Reservations List</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
	<th class="ducc-col-terminate"></th>
			<th title="The system assigned id for this reservation">Id</th>
			<th title="The time this reservation was submitted">Start</th>
			<th title="The time this reservation was finished">End</th>
			<th class="ducc-no-filter" id="user_column_heading" title="The user who submitted this reservation">User</th>
			<th title="The user specified class of this reservation">Class</th>
			<th title="The type of this reservation">Type</th>
			<th title="The current state of this reservation">State</th>
			<th title="The reason for the final state of this reservation, normally CanceledByUser">Reason</th>
			<th title="The number of resources (machines or shares, depending on class) assigned to this reservation">Allocation</th>
			<th title="The combined number of PIDs on the assigned resource(s) for user">User<br>Processes</th>
			<th title="The memory allocation per resource for this reservation, in GB">Size</th>
			<th title="The list of of machines assigned to this reservation">List</th>
			<th title="The user specified description of this reservation">Description</th>
	</tr>
	</thead>
	<tbody id="reservations_list_area">
	</tbody>
	</table>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
	<table width="100%">
   	<caption><b>Reservations List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th class="ducc-col-terminate"></th>
			<th title="The system assigned id for this reservation">Id</th>
			<th title="The time this reservation was submitted">Start</th>
			<th title="The time this reservation was finished">End</th>
			<th class="ducc-no-filter" id="user_column_heading" title="The user who submitted this reservation">User</th>
			<th title="The user specified class of this reservation">Class</th>
			<th title="The type of this reservation">Type</th>
			<th title="The current state of this reservation">State</th>
			<th title="The reason for the final state of this reservation, normally CanceledByUser">Reason</th>
			<th title="The number of resources (machines or shares, depending on class) assigned to this reservation">Allocation</th>
			<th title="The combined number of PIDs on the assigned resource(s) for user">User<br>Processes</th>
			<th title="The memory allocation per resource for this reservation, in GB">Size</th>
			<th title="The list of of machines assigned to this reservation">List</th>
			<th title="The user specified description of this reservation">Description</th>
		</tr>
		</thead>
		<tbody id="reservations_list_area">
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
