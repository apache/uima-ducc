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
  <%@ include file="$imports-classic.jsp" %>
  <script type="text/javascript">
	$(function() {
		$("#tabs").tabs();
	});
  </script>
  
</head>
<body onload="ducc_init('service-details');">

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
<%@ include file="$banner/c1-refresh-service-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c2-status-service-details.jsp" %>
</td>
<!-- *********************** column ************************ -->
<td valign="middle" align="center">
<%@ include file="$banner/c3-image-service-details.jsp" %>
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
<span id="service_summary_area"></span>
<tr>
<td valign="middle" colspan="5">

		<div id="tabs"> 
		<ul>
			<li><a href="#tabs-1">Deployments</a></li>
			<li><a href="#tabs-2">Registry</a></li>
			<li><a href="#tabs-3">Files</a></li>
			<li><a href="#tabs-4">History</a></li>
		</ul>
			<div id="tabs-1">
			    <%@ include file="service.details.table.deployments.jsp" %>
			</div>
			<div id="tabs-2">
				<div id="service_update_form_button">
				<button title="Hint: Login" disabled style="font-size:8pt;">Update</button>
				</div>
   				<div class="registry_data_div">
   					<span id="registry_data_area"></span>
   				</div>
			</div>
			<div id="tabs-3">
                <%@ include file="service.details.table.files.jsp" %>
            </div>
            <div id="tabs-4">
                <%@ include file="service.details.table.history.jsp" %>
            </div>
		</div>
</td>
</table>
</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
