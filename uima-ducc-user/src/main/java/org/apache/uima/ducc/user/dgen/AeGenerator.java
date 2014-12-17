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
package org.apache.uima.ducc.user.dgen;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;

public class AeGenerator {
	
	private String userLogDir = null;
	
	public AeGenerator(String userLogDir) {
		setUserLogDir(userLogDir);
	}
	
	public String generate(IDuccUimaDeployableConfiguration configuration, String jobId) throws Exception {
		IDuccUimaAggregate aggregateConfiguration = (IDuccUimaAggregate) configuration;
		return generateAe(aggregateConfiguration, jobId);
	}
	
	private String generateAe(IDuccUimaAggregate aggregateConfiguration, String jobId ) throws Exception {
		List<String> descriptorPaths = new ArrayList<String>();
		List<List<String>> overrides = new ArrayList<List<String>>();
		for( IDuccUimaAggregateComponent component: aggregateConfiguration.getComponents()) {
			descriptorPaths.add(component.getDescriptor());
			overrides.add(component.getOverrides());
		}
		String aed = createAED(
		    aggregateConfiguration.getName(), 
		    aggregateConfiguration.getDescription(), 
		    aggregateConfiguration.getBrokerURL(), 
		    aggregateConfiguration.getEndpoint(),	
		    aggregateConfiguration.getThreadCount(), 
		    userLogDir,
		    jobId+"-uima-ae-descriptor-"+Utils.getPID()+".xml",
			overrides, 
			descriptorPaths.toArray(new String[descriptorPaths.size()])
			);
		return aed;
	}

	private static String createAED (
			String name, 
			String description, 
			String brokerURL, 
			String endpoint,
			int scaleup, 
			String directory, 
			String fname, 
			List<List<String>> overrides,
			String... aeDescriptors) throws Exception {
		
		AnalysisEngineDescription aed = UimaUtils.createAggregateDescription((scaleup > 1),overrides, aeDescriptors);
		aed.getMetaData().setName(name);
		File file = null;
		File dir = new File(directory);
		if (!dir.exists()) {
			dir.mkdir();
		}
		FileOutputStream fos = null;
		try {
			file = new File(dir, fname);//+"-uima-ae-descriptor-"+Utils.getPID()+".xml");
			fos = new FileOutputStream(file);
			aed.toXML(fos);
			
		} 
		catch(Exception e) {
			throw e;
		} 
		finally {
			if( fos != null ) {
				fos.close();
			}
		}
		return file.getAbsolutePath();
	}
	
	private void setUserLogDir(String value) {
		userLogDir = value;
	}
}
