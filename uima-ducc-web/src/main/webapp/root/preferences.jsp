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
boolean dateStyle = true;
boolean descriptionStyle = true;
boolean displayStyle = true;
boolean role = true;
%>
<html>
<head>
  <link rel="shortcut icon" href="uima.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <script src="opensources/jquery-1.4.2.js" type="text/javascript"></script>
  <script src="opensources/jgrowl/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.8.4/gallery/jquery-ui-themes-1.8.4/themes/redmond/jquery-ui.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.8.4/ui/minified/jquery-ui.min.js"></script>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <script type="text/javascript" src="opensources/sorttable.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
</head>

<body onload="ducc_preferences();">

<table>

<tr>
<td valign="top">
  <div>
    <span>
      <h2><span class="title">ducc-mon</span></h2>
      <h4><span class="fulltitle">Distributed</span>
          <br>
          <span class="fulltitle">UIMA</span>
          <br>
          <span class="fulltitle">Cluster</span>
          <br>
          <span class="fulltitle">Computing</span>
          <br>
          <span class="fulltitle">Monitor</span>
          <span style="display:none">
          <br>
      	  <br>
      	  version: <span class="version" id="version"></span>
      	  </span>
      	  <br>
      	  <br>
      	  <span class="idtitle" id="identity"></span>
      </h4>
      <h3><span class="subtitle">Preferences<span></h3>
      
    </span>
    <span>
      <img src="opensources/images/duckquack-1.144x101px.jpg" style="border:3px solid #ffff7a" alt="logo">
    </span>
    <br>
    
<td valign="top"> 
  <div style="display:none">
    <span>
      <h2><span class="title">&nbsp </span></h2>
    </span>
    <h3>Refresh</h3>
    <table>
    <tr>
    <td>
      <div id="refreshbutton">
        <input type="image" onClick="location.reload()" title="Refresh" alt="Refresh" src="opensources/images/1284662827_refresh.png">
      </div>
    <td>
      <div>
        <form name="duccform">
		<input type="radio" name="refresh" value="manual"            onClick="ducc_put_cookie('ducc:refreshmode','manual'   )" /> Manual
		<input type="radio" name="refresh" value="automatic" checked onClick="ducc_put_cookie('ducc:refreshmode','automatic')" /> Automatic
		</form> 
      </div>
    </table>
    
  </div>
  
    <form name="form_preferences">
  	<div class="segment">
  	  <br/>
  	  <br/>
  	  <br/>
  	  <br/>
      <div class="preferences">
        <h3>User Preferences</h3>
        <table>
        <tr>
		<td><input type="button" name="reset"       value="Reset"          onclick="ducc_preferences_reset()" />
		<td>&nbsp
		<td><input type="button" name="done"        value="Done"           onclick="ducc_window_close()" />
		<td>&nbsp
		<td>&nbsp
		<td>&nbsp
		<td>&nbsp
		<tr>
        <tr>
        <td><i><b>Table Style</b></i>
		<td>&nbsp
		<td><input type="radio"  name="table_style" value="scroll" checked onclick="ducc_preferences_set('table_style','scroll')" /> Scroll
		<td>&nbsp
		<td><input type="radio"  name="table_style" value="classic"        onclick="ducc_preferences_set('table_style','classic')" /> Classic
		<td>&nbsp
		<td>&nbsp
<%
if (dateStyle) {
%>
		<tr>
		<tr>
        <td><i><b>Date Style</b></i>
		<td>&nbsp
		<td><input type="radio"  name="date_style" value="long" checked onclick="ducc_preferences_set('date_style','long')" /> Long
		<td>&nbsp
		<td><input type="radio"  name="date_style" value="medium"       onclick="ducc_preferences_set('date_style','medium')" /> Medium
		<td>&nbsp
		<td><input type="radio"  name="date_style" value="short"        onclick="ducc_preferences_set('date_style','short')" /> Short
<%
}
%>
<%
if (descriptionStyle) {
%>
		<tr>
		<tr>
        <td><i><b>Description Style</b></i>
		<td>&nbsp
		<td><input type="radio"  name="description_style" value="long" checked onclick="ducc_preferences_set('description_style','long')" /> Long
		<td>&nbsp
		<td><input type="radio"  name="description_style" value="short"        onclick="ducc_preferences_set('description_style','short')" /> Short
<%
}
%>
<%
if (displayStyle) {
%>
        <tr>
        <tr>
        <td><i><b>Display Style</b></i>
        <td>&nbsp
        <td><input type="radio"  name="display_style" value="textual" checked onclick="ducc_preferences_set('display_style','textual')" /> Textual
        <td>&nbsp
        <td><input type="radio"  name="display_style" value="visual"          onclick="ducc_preferences_set('display_style','visual')" /> Visual
<%
}
%>
		<tr>
        <tr>
        <td><i><b>Filter Users</b></i>
		<td>&nbsp
		<td><input type="radio"  name="filter_users_style" value="include" checked onclick="ducc_preferences_set('filter_users_style','include')" /> Include
		<td>&nbsp
		<td><input type="radio"  name="filter_users_style" value="include+active"  onclick="ducc_preferences_set('filter_users_style','include+active')" /> Include+Active
		<td>&nbsp
		<td><input type="radio"  name="filter_users_style" value="exclude" 		 onclick="ducc_preferences_set('filter_users_style','exclude')" /> Exclude
		<td>&nbsp
		<td><input type="radio"  name="filter_users_style" value="exclude+active"  onclick="ducc_preferences_set('filter_users_style','exclude+active')" /> Exclude+Active
		<td>&nbsp
		<td>&nbsp
<%
if (role) {
%>
		<tr>
		<tr>
        <td><i><b>Role</b></i>
		<td>&nbsp
		<td><input type="radio"  name="role" value="user"          checked onclick="ducc_preferences_set('role','user')" /> User
		<td>&nbsp
		<td><input type="radio"  name="role" value="administrator"         onclick="ducc_preferences_set('role','administrator')" /> Administrator
<%
}
%>		
        </table>
   	  </div>
    </div> 
    </form>   
   
  </div>

<tr>
<td>  
  <%@ include file="$copyright.jsp" %>
<td>

</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
