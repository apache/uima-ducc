/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.ws;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class DuccNodes {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccNodes.class.getName());
	private static DuccId jobid = null;
	
	private static DuccNodes duccNodes = new DuccNodes();
	
	private boolean sysout = false;
	
	public static DuccNodes getInstance() {
		return duccNodes;
	}
	
	public static DuccNodes getSysOutInstance() {
		DuccNodes retVal = new DuccNodes();
		retVal.sysout = true;
		return retVal;
	}
	
	private class NodeSet {
		public ArrayList<String> nodes = new ArrayList<String>();
		public ArrayList<String> imports = new ArrayList<String>();
	}
	
	private void warn(String location, DuccId jobid, String text) {
		if(sysout) {
			System.out.println(location+" "+text);
		}
		else {
			logger.warn(location, jobid, text);
		}
	}
	
	private void trace(String location, DuccId jobid, String text) {
		if(sysout) {
			System.out.println(location+" "+text);
		}
		else {
			logger.trace(location, jobid, text);
		}
	}
	
	private void ignore(String fileName, String reason) {
		String location = "ignore";
		String text = "file:"+fileName+" "+"reason:"+reason;
		warn(location, jobid, text);
	}
	
	private void ignore(String fileName, int lineno, String line, String reason) {
		String location = "ignore";
		String text = "file:"+fileName+" "+"#"+lineno+" "+line+" "+"reason:"+reason;
		warn(location, jobid, text);
	}
	
	private void duplicate(String fileName, int lineno, String line) {
		String location = "duplicate";
		String text = "file:"+fileName+" "+"#"+lineno+" "+line;
		trace(location, jobid, text);
	}
	
	private NodeSet get(String dirResources, String fileName, NodeSet nodeSet) {
		String filePath = fileName;
		if(!fileName.startsWith(File.separator)) {
			filePath = dirResources+fileName;
		}
		try {
		    BufferedReader in = new BufferedReader(new FileReader(filePath));
		    String line;
		    int lineno = 0;
		    while ((line = in.readLine()) != null) {
		    	lineno++;
		    	line = line.trim();
		    	if(line.length() == 0) {
		    	}
		    	else if(line.startsWith("#")) {
		    	}
		    	else {
		    		String[] nvp = line.split(" ");
		    		if(nvp.length == 1) {
		    			String value = nvp[0].trim();
		    			if(value.equals("import")) {
		    				ignore(filePath, lineno, line, "import file missing");
		    			}
		    			else {
		    				if(nodeSet.nodes.contains(value)) {
		    					duplicate(filePath, lineno, line);
		    				}
		    				else {
		    					nodeSet.nodes.add(value);
		    				}
		    			}
		    		}
		    		else if(nvp.length == 2) {
		    			String key = nvp[0].trim();
		    			String value = nvp[1].trim();
		    			if(key.equals("import")) {
		    				if(!value.startsWith(File.separator)) {
		    					value = dirResources+value;
		    				}
		    				if(nodeSet.imports.contains(value)) {
		    					duplicate(filePath, lineno, line);
		    				}
		    				else {
		    					get(dirResources, value, nodeSet);
		    				}
		    			}
		    			else {
		    				ignore(filePath, lineno, line, "expected import");
		    			}
		    		}
		    		// sim node
		    		else if(nvp.length == 3) {
		    			String value = nvp[1].trim()+"-"+nvp[0].trim();
		    			if(nodeSet.nodes.contains(value)) {
	    					duplicate(filePath, lineno, line);
	    				}
	    				else {
	    					nodeSet.nodes.add(value);
	    				}
		    		}
		    		else {
		    			ignore(filePath, lineno, line, "too many items on line");
		    		}
		    	}
		    }
		    in.close();
		} 
		catch (IOException e) {
			ignore(filePath, "file not found");
		}
		return nodeSet;
	}
	
	public ArrayList<String> get(String resourcesDir, String fileName) {
		NodeSet nodeSet = new NodeSet();
		get(resourcesDir, fileName, nodeSet);
		return nodeSet.nodes;
	}
	
	public static void main(String[] args) {
		String fileName = IDuccEnv.DUCC_NODES_FILE_NAME;
		String dirResources = IDuccEnv.DUCC_RESOURCES_DIR;
		ArrayList<String> nodes =  DuccNodes.getSysOutInstance().get(dirResources,fileName);
		Iterator<String> iterator = nodes.iterator();
		while(iterator.hasNext()) {
			String value = iterator.next();
			System.out.println(value);
		}
	}

}
