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
  <!-- Drop the sAjaxSource entry as it caused a superfluous request -->
  <script type="text/javascript" charset="utf-8">
	var oTable;
	$(document).ready(function() {
		oTable = $('#experiments-table').dataTable( {
		 dom: 'C<"clear">lfrtip',
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
			"bInfo": false,
			"aaSorting": [],
			"aoColumnDefs": [ { "bSortable": false, "aTargets": [ 0 ] } ],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
					 // right align the Termina-button, Duration & Tasks
					$('td:eq(0)', nRow).css( "text-align", "right" );
					$('td:eq(2)', nRow).css( "text-align", "right" );
                             		$('td:eq(4)', nRow).css( "text-align", "right" );
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
</head>
<body onload="ducc_init('experiments');" onResize="ducc_resize();">

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
<%@ include file="$banner/c1-refresh-experiments.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-experiments.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-experiments.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c4-ducc-mon.jsp" %>
</td>
</table>
<!-- *********************** /table ************************ -->
<!-- *********************** table ************************* -->
<%@ include file="$banner/t2-alerts.jsp" %>
<%@ include file="$banner/t2-messages.jsp" %>
<!-- *********************** /table ************************ -->
<!-- ####################### /common ####################### -->
<!-- @@@@@@@@@@@@@@@@@@@@@@@ unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
<%
if (table_style.equals("scroll")) {
%>
	<table width="100%">
	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Experiments List</b><br><i><small>click column heading to sort</small></i></caption>
	</table>
	<table id="experiments-table" width="100%">
	<thead>
	<tr class="ducc-header">
	<th class="ducc-col-terminate"></th>
	<th title="The time this experiment was first launched">Start</th>
	<th title="The duration time of this experiment" class="sorttable_numeric">Duration</th>
	<th title="The directory owner for this experiment">User</th>
	<th title="The number of tasks for this experiment">Tasks</th>
	<th title="The state of this experiment">State</th>
	<th title="The directory for this experiment">Directory</th>
	</tr>
	</thead>
	<tbody id="experiments_area">
	</tbody>
	</table>
<%
}
%>	
<%
if (table_style.equals("classic")) {
%>
	<table width="100%">
   	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Experiments List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th class="ducc-col-terminate"></th>
		<th title="The time this experiment was first launched">Start</th>
		<th title="The duration time of this experiment" class="sorttable_numeric">Duration</th>
		<th title="The directory owner for this experiment">User</th>
		<th title="The number of tasks for this experiment">Tasks</th>
		<th title="The state of this experiment">State</th>
		<th title="The directory for this experiment">Directory</th>
		</tr>
		</thead>
		<tbody id="experiments_area">
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
