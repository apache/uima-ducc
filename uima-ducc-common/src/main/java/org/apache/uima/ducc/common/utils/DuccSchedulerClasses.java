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

public class DuccSchedulerClasses {
	
	public static final String FAIR_SHARE = "FAIR_SHARE";
	public static final String FIXED_SHARE = "FIXED_SHARE";
	public static final String RESERVE = "RESERVE";
	public static final String JobDriver = "JobDriver";
	
	private static DuccSchedulerClasses instance = null;

	private long lastModified = 0;
    NodeConfiguration nodeConfiguration = null;
    DuccLogger logger = null;

	private String fileName = null;
	
	public static DuccSchedulerClasses getInstance() {
		if(instance == null) {
			instance = new DuccSchedulerClasses();
		}
		return instance;
	}
	
	public DuccSchedulerClasses() {
		String key = DuccPropertiesResolver.ducc_rm_class_definitions;
		String file_classes = DuccPropertiesResolver.getInstance().getFileProperty(key);
		String dir_home = Utils.findDuccHome();
		String dir_resources = "resources";
		fileName = dir_home+File.separator+dir_resources+File.separator+file_classes;
	}
	
    /**
     * Pass in a logger, usefull in the daemons.  If no logger the NodeConfiguration will
     * write to stdout/stderr.
     */
	public DuccSchedulerClasses(DuccLogger logger) {
        super();
        this.logger = logger;
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
        instance = getInstance();

        File file = new File(fileName);
        if ( lastModified != file.lastModified() ) {         // reread if it looks like it changed
            lastModified = file.lastModified();
            nodeConfiguration = new NodeConfiguration(fileName, logger);
            lastModified = file.lastModified();
            nodeConfiguration.readConfiguration();
        }

		return nodeConfiguration;
	}
    
    public Map<String, DuccProperties> getClasses()
    	throws Exception
    {
        readConfiguration();
        return nodeConfiguration.getClasses();
    }

	public boolean isPreemptable(String class_name)
		throws Exception
	{
		boolean retVal = false;
        readConfiguration();

        DuccProperties properties = nodeConfiguration.getClass(class_name);
		String policy = getProperty(properties, "policy");
		if(policy.equals(FAIR_SHARE)) {
			retVal = true;
		}
		return retVal;
	}
	
	public String getDefaultClassName() 
	    throws Exception
	{
		String retVal = null;
        readConfiguration();
        DuccProperties properties = nodeConfiguration.getDefaultFairShareClass();
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
        readConfiguration();
        DuccProperties properties = nodeConfiguration.getDefaultFixedClass();
        if (properties != null) {
            retVal = properties.getProperty("name");
        }
        return retVal;
    }
	
	public String getDebugClassSpecificName(String class_name)
	    throws Exception
	{
		String retVal = null;
        readConfiguration();
        DuccProperties properties = nodeConfiguration.getClass(class_name);
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
        readConfiguration();
        Map<String, DuccProperties> allClasses = nodeConfiguration.getClasses();
        ArrayList<String> classList = new ArrayList<String>();
        for ( DuccProperties p : allClasses.values() ) {
            String pol = p.getProperty("policy");
            String name = p.getProperty("name");
            if ( (pol.equals(RESERVE) || pol.equals(FIXED_SHARE)) && ( !name.equals(JobDriver) ) ) {
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
        readConfiguration();
        DuccProperties properties = nodeConfiguration.getDefaultReserveClass();
        if ( properties != null ) {
            retVal = properties.getProperty("name");
        }
		return retVal;

	}
}
