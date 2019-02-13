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
<html>
<head>
  <link rel="shortcut icon" href="uima.ico" />
  <title>ducc-mon</title>
  <meta http-equiv="CACHE-CONTROL" content="NO-CACHE">
  <script src="opensources/jquery-2.0.3.min.js" type="text/javascript"></script>
  <script src="opensources/jgrowl-1.3/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl-1.3/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.theme.min.css" rel="stylesheet" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.structure.min.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.11.0.custom/jquery-ui.min.js"></script>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <script type="text/javascript" src="opensources/sorttable.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
  <%@ include file="$imports.jsp" %>
</head>

<body onload="ducc_init('authentication-login');">
<table>
<tr>
<td valign="top" class="ducc-lhs">
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
          <br>
      	  <br>
      	  version: <span class="version" id="version"></span>
      	  <br>
      	  <br>
      	  <span class="idtitle" id="identity"></span>
      </h4>
      <h3><span class="subtitle">Authentication</span></h3>
      <h5><span class="authenticator">authenticator: </span><span class="authenticator" id="authenticator_version_area"></span>
          <br>
          <br>
          <span class="timestamptitle">updated: </span><span class="timestamptitle" id="timestamp_area"></span>
          <br>
          <span class="authenticationtitle">authentication status: </span><span class="authenticationtitle" id="authentication_area"></span>
      </h5>
    </span>
     <span>
      <%@ include file="$banner/c3-image-login.jsp" %>
    </span>
    <br>
  </div>
</td>  
<td valign="top"> 
  <div style="display:none">
    <span>
      <h2><span class="title">&nbsp; </span></h2>
    </span>
    <h3>Refresh</h3>
    <table>
    <tr>
    <td>
      <div id="refreshbutton">
        <input type="image" onClick="location.reload()" title="Refresh" alt="Refresh" src="opensources/images/refresh.png">
      </div>
    <td>
      <div>
        <form name="duccform">
		<input type="radio" name="refresh" value="manual"            onClick="ducc_put_cookie('DUCCrefreshmode','manual'   )" /> Manual
		<input type="radio" name="refresh" value="automatic" checked onClick="ducc_put_cookie('DUCCrefreshmode','automatic')" /> Automatic
		</form> 
      </div>
    </table>
  </div>
    <form id="login" name="login" method="post" action="/ducc-servlet/user-login">  
  	<div class="segment">
  	  <br/>
  	  <br/>
  	  <br/>
  	  <br/>
      <div class="login">
        <h3>Login</h3>
        <table>
        <tr>
        <td align="right">userid
        <td align="right">
        <td align="left" ><input id="userid" type="text" name="userid" />
        <tr>
        <td align="right">password
        <td align="right">
        <td align="left" >
          <span id="password_checked_area">
            <input id="password" type="password" name="password" disabled=disabled/>
          </span>
        <tr>
        <td align="right">
        <td align="right">
        <td align="left">
        <table>
        <tr>
        <td align="left" ><input id="login" type="button" onclick="ducc_submit_login()" value="Login" />
        <td align="left" ><input id="cancel" type="button" onclick="ducc_cancel_login()" value="Cancel" />
        </table>
        <tr>
        <td align="right">&nbsp
        <td align="right">
        <td align="left">
        <tr>
        <td align="right">&nbsp
        <td align="right">
        <td align="left">
        <tr>
        <td align="left" colspan="3">
	        <table>
	        <tr>
	        <td width="15%">
	        <td width="70%"><span class="authenticator" id="authenticator_notes_area"></span>
	        <td width="15%">
	        </table>
        </table>
        <br>
        <br>
   	  </div>
    </div>
    </form>
<tr>
<td>  
  <%@ include file="$copyright-narrow.jsp" %>
<td>
</table>
<script src="opensources/navigation/menu.js"></script>
</body>
</html>
