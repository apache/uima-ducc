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
package org.apache.uima.ducc.common.persistence.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class StateServices implements IStateServices {
	
	private static DuccLogger logger = DuccService.getDuccLogger(StateServices.class.getName());  
	private static DuccId jobid = null;
	
	private String directory_state_services = IDuccEnv.DUCC_STATE_SERVICES_DIR;
	
	StateServices() {
		mkdirs();
	}
	
	private void mkdirs() {
		IOHelper.mkdirs(directory_state_services);
	}
	
	private ArrayList<String> getList(String type) {
		
		String location = "getList";
		ArrayList<String> retVal = new ArrayList<String>();
		try {
			logger.debug(location, jobid, directory_state_services);
			File folder = new File(directory_state_services);
			File[] listOfFiles = folder.listFiles();
			if(listOfFiles != null) {
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						String name = listOfFiles[i].getName();
						if(name.endsWith("."+type)) {
							retVal.add(name);
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

	
	public ArrayList<String> getSvcList() {
		return getList(IStateServices.svc);
	}

	
	public ArrayList<String> getMetaList() {
		return getList(IStateServices.meta);
	}
	
	private Properties getProperties(String name) {
		String location = "getProperties";
		Properties properties = new Properties();
		try {
			FileInputStream fis = new FileInputStream(name);
			try {
				properties.load(fis);
			}
			finally {	
				fis.close();
			}
		}
		catch(Exception e) {	
			logger.error(location, jobid, e);
		}
		return properties;
	}
	
	
	public StateServicesDirectory getStateServicesDirectory() throws IOException {
		String location = "getStateServicesDirectory";
		StateServicesDirectory ssd = null;
		try {
			ssd = new StateServicesDirectory();
			ArrayList<String> svcList = getSvcList();
			logger.trace(location, jobid, svcList.size());
			for(String entry : svcList) {
				try {
					StateServicesSet sss = new StateServicesSet();
					String num = entry.split("[.]")[0];
					Integer i = new Integer(num);
					String base = directory_state_services+num;
					logger.trace(location, jobid, base);
					String fnSvc = base+"."+svc;
					String fnMeta = base+"."+meta;
					Properties propertiesSvc = getProperties(fnSvc);
					sss.put(svc, propertiesSvc);
					Properties propertiesMeta = getProperties(fnMeta);
					sss.put(meta, propertiesMeta);
					ssd.put(i, sss);
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return ssd;
	}
	
}
