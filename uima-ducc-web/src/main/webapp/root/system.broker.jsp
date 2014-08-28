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
		oTable = $('#system-broker').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "/ducc-servlet/json-format-aaData-broker",
			aaSorting: [],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
			                        $('td:eq(2)' , nRow).css( "text-align", "right" );
                                    $('td:eq(3)' , nRow).css( "text-align", "right" );
                                    $('td:eq(4)' , nRow).css( "text-align", "right" );
                                    $('td:eq(5)' , nRow).css( "text-align", "right" );
                                    $('td:eq(6)' , nRow).css( "text-align", "right" );
                             		return nRow;
			},
		} );
	} );
  </script>
<%
}
%>	
</head>
<body onload="ducc_init('system-broker');" onResize="ducc_resize();">

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
<%@ include file="$banner/c1-refresh-system-broker.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-system-broker.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-system-broker.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c4-ducc-mon.jsp" %>
</td>
</table>
<!-- *********************** /table ************************ -->
</div>
<!-- ####################### /common ####################### -->

<!-- @@@@@@@@@@@@@@@@@@@@@@@ unique @@@@@@@@@@@@@@@@@@@@@@@@ -->

<table class="body">
<tr>
<td valign="middle" colspan="5">
<span id="broker_summary_area"></span>
<tr>
<td valign="middle" colspan="5">

<%
if (table_style.equals("scroll")) {
%>
	<table width="100%">
	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Broker Info</b><br><i><small>click column heading to sort</small></i></caption>
	</table>
	<table id="system-broker" width="100%">
	<thead>
	<tr class="ducc-header">
	    <th align="center" >Name</th>
	    <th align="center" >Type</th>
        <th align="center" >ConsumerCount</th>
        <th align="center" >QueueSize</th>
        <th align="center" >MaxEnqueueTime</th>
        <th align="center" >AverageEnqueueTime</th>
        <th align="center" >MemoryPercentUsage</th>
	</tr>
	</thead>
	<tbody id="system_broker_list_area">
	</tbody>
	</table>
<%
}
%>   
<%
if (table_style.equals("classic")) {
%>
	<table id="system-broker" width="100%">
	<caption title="Hint: use Preferences -> Table Style to alter format"><b>Broker Info</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th align="center" >Name</th>
		<th align="center" >Type</th>
        <th align="center" >ConsumerCount</th>
        <th align="center" >QueueSize</th>
        <th align="center" >MaxEnqueueTime</th>
        <th align="center" >AverageEnqueueTime</th>
        <th align="center" >MemoryPercentUsage</th>
		</tr>
		</thead>
		<tbody id="system_broker_list_area">
   		</tbody>
	  </table>
   	</table>
<%
}
%>	    

</table>

<!-- @@@@@@@@@@@@@@@@@@@@@@@ /unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
		
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
