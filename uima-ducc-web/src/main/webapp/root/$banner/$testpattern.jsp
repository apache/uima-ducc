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

<%@page import="java.io.File" %>
<%@page import="java.io.InputStream" %>
<%@page import="java.util.Properties" %>

<%
	String testpattern_uri = request.getRequestURI();
	int testpattern_start = testpattern_uri.lastIndexOf("/")+1;
	int tertpattern_end = testpattern_uri.indexOf("?");
	String testpattern_key = "test.pattern";
	String testpattern_folder = "/resources";
	String testpattern_image = testpattern_folder+"/UIMA.png";
	try {
		InputStream stream = application.getResourceAsStream(testpattern_folder+"/image-map.properties");
	    Properties props = new Properties();
	    props.load(stream);
	    String value = props.getProperty(testpattern_key);
	    if(value != null) {
	    	value = value.trim();
	    	if(value.length() > 0) {
	    		String fileName = value;
	    		if(!fileName.startsWith(File.pathSeparator)) {
	    			fileName = application.getRealPath(value);
	    		}
	    		File file = new File(fileName);
	    		if(file.exists()) {
	    			testpattern_image = value;
	    		}
	    	}
	    }
	}
	catch(Exception e) {
	}  
%>

<style>

img#spin { 
    -moz-animation:60s rotateRight infinite linear; 
    -webkit-animation:60s rotateRight infinite linear; 
}

@-moz-keyframes rotateRight{
    0%{ -moz-transform:rotate(0deg); -moz-transform-origin:50% 50%; }
    100%{ -moz-transform:rotate(360deg); }
}

@-webkit-keyframes rotateRight{
    0%{ -webkit-transform:rotate(0deg); -webkit-transform-origin:50% 50%; }
    100%{ -webkit-transform:rotate(360deg); }
}

</style>

<td valign="middle" align="center">
<table>
<tr>
<td>
<table>
<tr><td style="color: red;">T
<tr><td style="color: red;">E
<tr><td style="color: red;">S
<tr><td style="color: red;">T
</table>
<td>&nbsp
<td>
<img id="spin" src="<%=testpattern_image%>" >
<td>&nbsp
<td>
<table>
<tr><td style="color: red;">T
<tr><td style="color: red;">E
<tr><td style="color: red;">S
<tr><td style="color: red;">T
</table>
</table>
</td>