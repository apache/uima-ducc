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
	$(document).ready(function() {
		oTable = $('#machines-table').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-machines",
			aaSorting: [],
			"fnRowCallback" : function(nRow,aData,iDisplayIndex) {
                            $('td:eq(3)' , nRow).css( "text-align", "right" );
                            $('td:eq(4)' , nRow).css( "text-align", "right" );
                            $('td:eq(5)' , nRow).css( "text-align", "right" );
                            $('td:eq(6)' , nRow).css( "text-align", "right" );
                            $('td:eq(7)' , nRow).css( "text-align", "right" );
                            $('td:eq(8)' , nRow).css( "text-align", "right" );
                            $('td:eq(9)' , nRow).css( "text-align", "right" );                            
                            if ( aData[0] == "up" ) {
                    			jQuery('td:eq(0)', nRow).addClass('health_green');
                			}
                			if ( aData[0] == "down" ) {
                    			jQuery('td:eq(0)', nRow).addClass('health_red');
                			}
                			try {
                				if( parseInt(aData[4]) > 0) {
                					jQuery('td:eq(4)', nRow).addClass('health_red');
                				}
                			}
                			catch(err) {
							}
                            return nRow;
			},
		} );
	} );
    </script>
  <%
  }
  %>	
</head>

<body onload="ducc_init('system-machines');" onResize="window.location.href = window.location.href;">

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
<%@ include file="$banner/c1-refresh-system-machines.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-system-machines.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-system-machines.jsp" %>
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
<%
if (table_style.equals("scroll")) {
%>
	<table id="machines-table" width="100%">
	<caption><b>Machines List</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
	<th align="left" title="The current status">Status</th>
	<th align="left" title="The host IP">IP</th>
	<th align="left" title="The host name">Name</th>
	<th align="left" title="The host reservation size (hint: specify this size to reserve entire machine), in GB" >Reserve(GB):size</th>
	<th align="left" title="The host memory size, in GB" >Memory(GB):total</th>
	<th align="left" title="The host inuse swap size, in GB" >Swap(GB):inuse</th>
	<th align="left" title="The number of alien PIDs (those processes not associated with a user running jobs or having a reservation)" >Alien PIDs</th>
	<th align="left" title="The number of system shares" >Shares:total</th>
	<th align="left" title="The number of system shares inuse" >Shares:inuse</th>
	<th align="left" title="The elapsed time (in seconds) since the last heartbeat" >Heartbeat (last)</th>
	</tr>
	</thead>
	<tbody id="machines_list_area">
	</tbody>
	</table>
<%
}
%> 
<%
if (table_style.equals("classic")) {
%>
	<table>
   	<caption><b>Machines List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th align="left" title="The current status">Status</th>
		<th align="left" title="The host IP">IP</th>
		<th align="left" title="The host name">Name</th>
		<th align="left" title="The host reservation size (hint: specify this size to reserve entire machine), in GB" >Reserve(GB):size</th>
        <th align="left" title="The host memory size, in GB" >Memory(GB):total</th>	
		<th align="left" title="The host inuse swap size, in GB" >Swap(GB):inuse</th>
		<th align="left" title="The number of alien PIDs (those processes not associated with a user running jobs or having a reservation)" >Alien PIDs</th>
		<th align="left" title="The number of system shares" >Shares:total</th>
		<th align="left" title="The number of system shares inuse" >Shares:inuse</th>
		<th align="left" title="The elapsed time (in seconds) since the last heartbeat" >Heartbeat (last)</th>
		</tr>
		</thead>
		<tbody id="machines_list_area">
   		</tbody>
	  </table>
   	</table>
<%
}
%>	    
<!-- @@@@@@@@@@@@@@@@@@@@@@@ /unique @@@@@@@@@@@@@@@@@@@@@@@@ -->
		
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
