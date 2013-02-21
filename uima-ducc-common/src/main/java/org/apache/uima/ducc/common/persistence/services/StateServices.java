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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.Utils;


public class StateServices implements IStateServices {

	private static StateServices instance = new StateServices();
	
	public static StateServices getInstance() {
		return instance;
	}
	
	/*
	private static final DuccLogger logger = DuccLoggerComponents.getTrLogger(ServiceDefinitionsProperties.class.getName());
	*/
	
	private String directory_state_services = IDuccEnv.DUCC_STATE_SERVICES_DIR;
	
	/*
	private enum Verbosity {
		QUIET,
		SPEAK,
	}
	*/
	
	public StateServices() {
		mkdirs();
	}
	
	private void mkdirs() {
		IOHelper.mkdirs(directory_state_services);
	}
	
	private ArrayList<String> getList(String type) {
		ArrayList<String> retVal = new ArrayList<String>();
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
		return retVal;
	}

	@Override
	public ArrayList<String> getSvcList() {
		return getList(IStateServices.svc);
	}

	@Override
	public ArrayList<String> getMetaList() {
		return getList(IStateServices.meta);
	}
	
	private Properties getProperties(String name) {
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
		}
		return properties;
	}
	
	@Override
	public StateServicesDirectory getStateServicesDirectory() throws IOException {
		StateServicesDirectory ssd = new StateServicesDirectory();
		ArrayList<String> svcList = getSvcList();
		for(String entry : svcList) {
			try {
				StateServicesSet sss = new StateServicesSet();
				String num = entry.split("[.]")[0];
				Integer i = new Integer(num);
				String fnSvc = directory_state_services+num+"."+svc;
				String fnMeta = directory_state_services+num+"."+meta;
				Properties propertiesSvc = getProperties(fnSvc);
				sss.put(svc, propertiesSvc);
				Properties propertiesMeta = getProperties(fnMeta);
				sss.put(meta, propertiesMeta);
				ssd.put(i, sss);
			}
			catch(Exception e) {
			}
		}
		return ssd;
	}
	
	///// <test>
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String ducc_home = Utils.findDuccHome();
		if(ducc_home == null) {
			System.out.println("DUCC_HOME not set in environment");
			return;
		}
		if(ducc_home.trim() == "") {
			System.out.println("DUCC_HOME not set in environment");
			return;
		}
		StateServices ss = StateServices.getInstance();
		ArrayList<String> svcList = ss.getSvcList();
		for(String fname : svcList) {
			System.out.println(fname);
		}
		ArrayList<String> metaList = ss.getMetaList();
		for(String fname : metaList) {
			System.out.println(fname);
		}
		StateServicesDirectory ssd = ss.getStateServicesDirectory();
		NavigableSet<Integer> keySet = ssd.getDescendingKeySet();
		Iterator<Integer> iterator = keySet.iterator();
		while(iterator.hasNext()) {
			Integer i = iterator.next();
			StateServicesSet sss = ssd.get(i);
			Properties propertiesSvc = sss.get(svc);
			Iterator<Entry<Object, Object>> iteratorSvc = propertiesSvc.entrySet().iterator();
			while(iteratorSvc.hasNext()) {
				Entry<Object, Object> entrySvc = iteratorSvc.next();
				System.out.println(svc+":"+entrySvc.getKey()+"="+entrySvc.getValue());
			}
			Properties propertiesMeta = sss.get(meta);
			Iterator<Entry<Object, Object>> iteratorMeta = propertiesMeta.entrySet().iterator();
			while(iteratorMeta.hasNext()) {
				Entry<Object, Object> entryMeta = iteratorMeta.next();
				System.out.println(meta+":"+entryMeta.getKey()+"="+entryMeta.getValue());
			}
		}
	}

	///// </test>
}
