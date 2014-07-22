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
  <script src="opensources/jquery-2.0.3.min.js" type="text/javascript"></script>
  <script src="opensources/cluetip/jquery.cluetip.js" type="text/javascript"></script>
  <link href="opensources/cluetip/jquery.cluetip.css" rel="stylesheet" type="text/css">
<%
if (table_style.equals("scroll")) {
%>
  <script type="text/javascript" language="javascript" src="opensources/DataTables-1.10.1/media/js/jquery.dataTables.min.js"></script>
  <script type="text/javascript" language="javascript" src="opensources/DataTables-plugins/fnReloadAjax.js"></script>
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
  <script src="opensources/jgrowl-1.3/jquery.jgrowl.js" type="text/javascript"></script>
  <link rel="stylesheet" href="opensources/jgrowl-1.3/jquery.jgrowl.css" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.theme.min.css" rel="stylesheet" type="text/css"/>
  <link href="opensources/jquery-ui-1.11.0.custom/jquery-ui.structure.min.css" rel="stylesheet" type="text/css"/>
  <script src="opensources/jquery-ui-1.11.0.custom/jquery-ui.min.js"></script>ond/jquery-ui.css" rel="stylesheet" type="text/css"/>
  <link href="opensources/navigation/menu.css" rel="stylesheet" type="text/css">
  <script src="js/ducc.js"></script>
  <link href="ducc.css" rel="stylesheet" type="text/css">
