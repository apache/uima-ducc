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
		oTable = $('#services-definitions-table').dataTable( {
			"bProcessing": true,
			"bPaginate": false,
			"bFilter": true,
			"sScrollX": "100%",
			"sScrollY": "600px",
       		"bInfo": false,
			"sAjaxSource": "ducc-servlet/json-format-aaData-services-definitions",
			"aaSorting": [],
			"fnRowCallback"  : function(nRow,aData,iDisplayIndex) {
                             		$('td:eq(0)', nRow).css( "text-align", "right" );
                             		$('td:eq(2)', nRow).css( "text-align", "right" );
                             		$('td:eq(5)', nRow).css( "text-align", "right" );
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
<body onload="ducc_init('services-definitions');" onResize="window.location.href = window.location.href;">

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
<%@ include file="$banner/c1-refresh-services-definitions.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-services-definitions.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-services-definitions.jsp" %>
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
	<table id="services-definitions-table" width="100%">
   	    <caption><b>Services Definitions List</b><br><i><small>click column heading to sort</small></i></caption>
			<thead>
			<tr class="ducc-header">
			<th align="left" title="The service Id">Id</th>
			<th align="left" title="The service endpoint">Endpoint</th>
			<th align="left" title="The service number of instances">Instances</th>
			<th align="left" class="ducc-no-filter" id="user_column_heading" title="The service owning user">Owning User</th>
			<th align="left" title="The service scheduling class">Scheduling<br>Class</th>
			<th align="left" title="The service process memory size (GB)">Size</th>
			<th align="left" title="The service description">Description</th>
			</tr>
			</thead>
			<tbody id="services_list_area">
   	  		</tbody>
   	</table>
-->   	
	<table id="services-definitions-table" width="100%">
	<caption><b>Services Definitions List</b><br><i><small>click column heading to sort</small></i></caption>
	<thead>
	<tr class="ducc-header">
		<th align="left" title="The service Id">Id</th>
		<th align="left" title="The service endpoint">Endpoint</th>
		<th align="left" title="The service number of instances">Instances</th>
		<th align="left" class="ducc-no-filter" id="user_column_heading" title="The service owning user">Owning User</th>
		<th align="left" title="The service scheduling class">Scheduling<br>Class</th>
		<th align="left" title="The service process memory size (GB)">Size</th>
		<th align="left" title="The service description">Description</th>
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
   	<caption><b>Services Definitions List</b><br><i><small>click column heading to sort</small></i></caption>
   	<tr>
    <td>
      <table class="sortable">
		<thead>
		<tr class="ducc-head">
		<th align="left" title="The service Id">Id</th>
		<th align="left" title="The service endpoint">Endpoint</th>
		<th align="left" title="The service number of instances">Instances</th>
		<th align="left" class="ducc-no-filter" id="user_column_heading" title="The service owning user">Owning User</th>
		<th align="left" title="The service scheduling class">Scheduling<br>Class</th>
		<th align="left" title="The service process memory size (GB)">Size</th>
		<th align="left" title="The service description">Description</th>
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
