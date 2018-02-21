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
	String uri = request.getRequestURI();
	int start = uri.lastIndexOf("/")+1;
	int end = uri.indexOf("?");
	String pageName = uri.substring(start);
	if(end > 0) {
		pageName = uri.substring(start,end);
	}
	String folder = "/resources";
	String image = folder+"/UIMA_banner2tlpTm.png";
	try {
		InputStream stream = application.getResourceAsStream(folder+"/image-map.properties");
	    Properties props = new Properties();
	    props.load(stream);
	    String value = props.getProperty(pageName);
	    if(value != null) {
	    	value = value.trim();
	    	if(value.length() > 0) {
	    		String fileName = value;
	    		if(!fileName.startsWith(File.pathSeparator)) {
	    			fileName = application.getRealPath(value);
	    		}
	    		File file = new File(fileName);
	    		if(file.exists()) {
	    			image = value;
	    		}
	    	}
	    }
	}
	catch(Exception e) {
	}  
%>