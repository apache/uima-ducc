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
package org.apache.uima.ducc.common.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccSchedulerClasses {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccSchedulerClasses.class.getName());
	private static DuccId jobid = null;
	
	public static final String FAIR_SHARE = "FAIR_SHARE";
	public static final String FIXED_SHARE = "FIXED_SHARE";
	public static final String RESERVE = "RESERVE";
	public static final String JobDriver = "JobDriver";
	
	private static DuccSchedulerClasses instance = null;
	 
	private long lastModified = 0;
	private NodeConfiguration nodeConfiguration = null;

	private String fileName = null;
	
	public static DuccSchedulerClasses getInstance() {
	    // Use double check locking for safety
		if (instance == null) {
		    synchronized(DuccSchedulerClasses.class){
                if (instance == null) {
                    instance = new DuccSchedulerClasses();
                }
		    }
		}
		return instance;
	}

	private DuccSchedulerClasses() {
	    String dir_home = Utils.findDuccHome();  // Ensure DUCC_HOME is in the System properties
		String key = DuccPropertiesResolver.ducc_rm_class_definitions;
		String file_classes = DuccPropertiesResolver.getInstance().getFileProperty(key);
		String dir_resources = "resources";
		fileName = dir_home+File.separator+dir_resources+File.separator+file_classes;
	}
	
	public String getProperty(Properties properties, String name) {
        if ( properties == null ) return null;

		String retVal = "";
		String property = properties.getProperty(name);
		if(property != null) {
			retVal = property.trim();
		}
		return retVal;
	}
	
	public NodeConfiguration readConfiguration() 
        throws Exception
    {
        File file = new File(fileName);
        if ( lastModified != file.lastModified() ) {         // reread if it looks like it changed
            synchronized(this) {    // Ensure parallel threads see a valid nodeConfiguration 
                if ( lastModified != file.lastModified() ) { // an earlier thread may have already done the work
                    nodeConfiguration = new NodeConfiguration(fileName, null, null, logger); // UIMA-4275 use single common constructor
                    nodeConfiguration.readConfiguration();
                    lastModified = file.lastModified();   // Update this AFTER the nodeConfiguration is valid
                }
            }
        }
		return nodeConfiguration;
	}
    
    public Map<String, DuccProperties> getClasses()
    	throws Exception
    {
        return readConfiguration().getClasses();
    }

  public boolean isPreemptable(String class_name) throws Exception {
    boolean retVal = false;

    DuccProperties properties = readConfiguration().getClass(class_name);
    if (properties == null) {
      throw new IllegalArgumentException("Invalid scheduling_class: " + class_name);
    }
    String policy = getProperty(properties, "policy");
    if (policy.equals(FAIR_SHARE)) {
      retVal = true;
    }
    return retVal;
  }
	
	/**
	 * Get nodepool for specified node, else empty string
	 */
	public String getNodepool(String node) {
		String location = "getNodepool";
		String retVal = "";
		String nodepool = null;
		try {
			if(node != null) {
				NodeConfiguration nc = readConfiguration();
				// first try fully qualified node name
				nodepool = nc.getNodePoolNameForNode(node);
				if(nodepool == null) {
					// second try domainless node name
					String domainlessNode = node.split("\\.")[0];
					nodepool = nc.getNodePoolNameForNode(domainlessNode);
					if(nodepool == null) {
						// third try default node pool
						nodepool = nc.getFirstNodepool();
					}
				}
				if(nodepool != null) {
					retVal = nodepool;
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
  
	public String getDefaultClassName() 
	    throws Exception
	{
		String retVal = null;
        DuccProperties properties = readConfiguration().getDefaultFairShareClass();
        if ( properties != null ) {
            retVal = properties.getProperty("name");
        }
		return retVal;
	}
	
    /**
     * Need a default debug class for debug jobs with no scheduling class
     */
    public String getDebugClassDefaultName() throws Exception {
        String retVal = null;
        DuccProperties properties = readConfiguration().getDefaultFixedClass();
        if (properties != null) {
            retVal = properties.getProperty("name");
        }
        return retVal;
    }
	
	public String getDebugClassSpecificName(String class_name)
	    throws Exception
	{
		String retVal = null;
        DuccProperties properties = readConfiguration().getClass(class_name);
        if ( properties != null ) {
            retVal = properties.getProperty("debug");
        }
        if (retVal == null) {
            retVal = getDebugClassDefaultName();
        }
	    return retVal;
	}
	
	public String[] getReserveClasses()
        throws Exception
    {
        Map<String, DuccProperties> allClasses = readConfiguration().getClasses();
        ArrayList<String> classList = new ArrayList<String>();
        for ( DuccProperties p : allClasses.values() ) {
            String pol = p.getProperty("policy");
            String name = p.getProperty("name");
            if ( (pol.equals(RESERVE)) && (!name.equals(JobDriver)) ) {
                classList.add(p.getProperty("name"));
            }
        }

		String[] retVal = classList.toArray(new String[0]);
		return retVal;
	}
	
	public String[] getFixedClasses()
        throws Exception
    {
        Map<String, DuccProperties> allClasses = readConfiguration().getClasses();
        ArrayList<String> classList = new ArrayList<String>();
        for ( DuccProperties p : allClasses.values() ) {
            String pol = p.getProperty("policy");
            String name = p.getProperty("name");
            if ( (pol.equals(FIXED_SHARE)) && (!name.equals(JobDriver)) ) {
                classList.add(p.getProperty("name"));
            }
        }

		String[] retVal = classList.toArray(new String[0]);
		return retVal;
	}
	
	public String getReserveClassDefaultName()
		throws Exception
	{
		String retVal = "";
        DuccProperties properties = readConfiguration().getDefaultReserveClass();
        if ( properties != null ) {
            retVal = properties.getProperty("name");
        }
		return retVal;

	}
}
