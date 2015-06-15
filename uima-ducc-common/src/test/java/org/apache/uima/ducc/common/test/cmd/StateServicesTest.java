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
package org.apache.uima.ducc.common.test.cmd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesFactory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.Utils;

public class StateServicesTest 
//	extends StateServices
{
	
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
		IStateServices ss = StateServicesFactory.getInstance();
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
			String svc = IStateServices.svc;
			String meta = IStateServices.meta;
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

}
