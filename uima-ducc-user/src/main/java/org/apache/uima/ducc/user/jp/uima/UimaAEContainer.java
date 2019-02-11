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

package org.apache.uima.ducc.user.jp.uima;

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.CasPool;
import org.apache.uima.util.XMLInputSource;

public class UimaAEContainer {
	public static ResourceManager rm = 
			UIMAFramework.newDefaultResourceManager();
	private CasPool casPool = null;
	// Map to store DuccUimaSerializer instances. Each has affinity to a thread
	private Map<Long, org.apache.uima.aae.UimaSerializer> serializerMap =
					new HashMap<>();
	// Platform MBean server if one is available (Java 1.5 only)
	ThreadLocal<AnalysisEngine> threadLocal=null;
	public UimaAEContainer( ThreadLocal<AnalysisEngine> threadLocal ) {
		this.threadLocal = threadLocal;
	}
	public void initializeAe(String analysisEngineDescriptor, Object platformMBeanServer) throws Exception {
	    HashMap<String,Object> paramsMap = new HashMap<>();
        paramsMap.put(Resource.PARAM_RESOURCE_MANAGER, rm);
	    paramsMap.put(AnalysisEngine.PARAM_MBEAN_SERVER, platformMBeanServer);

		XMLInputSource is =
				UimaUtils.getXMLInputSource(analysisEngineDescriptor);
		String aed = is.getURL().toString();
		ResourceSpecifier rSpecifier =
		    UimaUtils.getResourceSpecifier(aed);

		AnalysisEngine ae = (AnalysisEngine)UIMAFramework.produceAnalysisEngine(rSpecifier,
				paramsMap);
		threadLocal.set(ae);
		
//		return UIMAFramework.produceAnalysisEngine(rSpecifier,
//				paramsMap);
		
	}

}
