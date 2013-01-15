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
  <link rel="shortcut icon" href="ducc.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <script src="opensources/jquery-1.4.2.js" type="text/javascript"></script>
<%
if (table_style.equals("scroll")) {
%>
  <script type="text/javascript" language="javascript" src="opensources/DataTables-1.9.1/media/js/jquery.dataTables.min.js"></script>
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
		oTable = $('#system-daemons').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-system-daemons-data",
			aaSorting: [],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
                             		$('td:eq(5)', nRow).css( "text-align", "right" );
                             		$('td:eq(6)', nRow).css( "text-align", "right" );
                             		$('td:eq(7)', nRow).css( "text-align", "right" );
                             		$('td:eq(8)', nRow).css( "text-align", "right" );
                             		$('td:eq(9)', nRow).css( "text-align", "right" );
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
<body onload="ducc_init('system-daemons');" onResize="window.location.href = window.location.href;">

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
<div id="show_agents_form_button">
<button id="showbutton" style="font-size:8pt;" onclick="ducc_show_agents()">Show<br>Agents</button>
</div>
<div id="hide_agents_form_button">
<button id="hidebutton" style="font-size:8pt;" onclick="ducc_hide_agents()">Hide<br>Agents</button>
</div>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c1-refresh-system-daemons.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-system-daemons.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-system-daemons.jsp" %>
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
	<table id="system-daemons" width="100%">
	<caption><b>Daemons List</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
	<th align="left" style="width: 75px;" title="The current status">Status</th>
	<th align="left" title="The daemon name">Daemon Name</th>
	<th align="left" style="width: 150px;" title="The date+time this daemon was booted">Boot Time</th>
	<th align="left" style="width: 75px;"title="The host IP for this daemon">Host IP</th>
	<th align="left" title="The host name for this daemon">Host Name</th>
	<th align="left" style="width: 75px;" title="The OS assigned PID for this daemon">PID</th>
	<th align="left" style="width: 75px;"title="The size (in bytes) of the most recent publication by this daemon">Publication Size (last)</th>
	<th align="left" style="width: 75px;"title="The size (in bytes) of the largest publication by this daemon">Publication Size (max)</th>
	<th align="left" title="The elapsed time (in seconds) since the last heartbeat from this daemon">Heartbeat (last)</th>
	<th align="left" title="The elapsed time (in seconds) of the longest heartbeat from this daemon">Heartbeat (max)</th>
	<th align="left" title="The TOD when the longest heartbeat from this daemon occurred">Heartbeat (max) TOD</th>
	<th align="left" style="width: 300px;" title="The JConsole URL for this daemon">JConsole: URL</th>
	</tr>
	</thead>
	<tbody id="system_daemons_list_area">
	</tbody>
	</table>
<%
}
%>   
<%
if (table_style.equals("classic")) {
%>
	<table>
   	<caption><b>Daemons List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<thead>
		<tr class="ducc-head">
			<th align="left" style="width: 75px;" title="The current status">Status</th>
			<th align="left" title="The daemon name">Daemon Name</th>
			<th align="left" style="width: 150px;" title="The date+time this daemon was booted">Boot Time</th>
			<th align="left" style="width: 75px;"title="The host IP for this daemon">Host IP</th>
			<th align="left" title="The host name for this daemon">Host Name</th>
			<th align="left" style="width: 75px;" title="The OS assigned PID for this daemon">PID</th>
			<th align="left" style="width: 75px;"title="The size (in bytes) of the most recent publication by this daemon">Publication Size (last)</th>
			<th align="left" style="width: 75px;"title="The size (in bytes) of the largest publication by this daemon">Publication Size (max)</th>
			<th align="left" title="The elapsed time (in seconds) since the last heartbeat from this daemon">Heartbeat (last)</th>
			<th align="left" title="The elapsed time (in seconds) of the longest heartbeat from this daemon">Heartbeat (max)</th>
			<th align="left" title="The TOD when the longest heartbeat from this daemon occurred">Heartbeat (max) TOD</th>
			<th align="left" style="width: 300px;" title="The JConsole URL for this daemon">JConsole: URL</th>
		</tr>
		</thead>
		<tbody id="system_daemons_list_area">
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
