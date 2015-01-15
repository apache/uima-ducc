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
package org.apache.uima.ducc.user.dgen.iface;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.user.dgen.AeGenerator;
import org.apache.uima.ducc.user.dgen.DuccUimaAggregate;
import org.apache.uima.ducc.user.dgen.DuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dgen.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dgen.IDuccUimaDeployableConfiguration;

public class AeGenerate implements IAeGenerate {

	public AeGenerate() {	
	}
	
	private void conditionalAddComponent(ArrayList<IDuccUimaAggregateComponent> dgenComponents, String descriptor, List<String> overrides) {
		if(descriptor != null) {
			DuccUimaAggregateComponent dgenComponent = new DuccUimaAggregateComponent(descriptor, overrides);
			dgenComponents.add(dgenComponent);
		}
	}
	
	private void show(String text) {
		System.out.println(text);
	}
	
	private void show(String name, String value) {
		show(name+"="+value);
	}
	
	private void show(String name, List<String> value) {
		if(value == null) {
			show(name+"="+value);
		}
		else {
			show(name+"="+value.toString());
		}
	}
	
	private String fabricateTargetDirectoryName(String baseDir, String jobId) {
		StringBuffer sb = new StringBuffer();
		if(baseDir != null) {
			sb.append(baseDir);
			if(!baseDir.endsWith(File.separator)) {
				sb.append(File.separator);
			}
			if(jobId != null) {
				sb.append(jobId);
			}
		}
		return sb.toString();
	}
	
	@Override
	public String generate(
			String directory, 
			String id,
			String dgenName,
			String dgenDescription,
			Integer dgenThreadCount,
			String dgenBrokerURL,
			String dgenEndpoint,
			String dgenFlowController,
			String cmDescriptor,
			List<String> cmOverrides, 
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor,
			List<String> ccOverrides
			) throws AeException
	{
		String retVal = null;
		try {
			show("directory", directory);
			show("id", id);
			show("dgenName", dgenName);
			show("dgenDescription", dgenDescription);
			show("dgenThreadCount", dgenThreadCount.toString());
			show("dgenBrokerURL", dgenBrokerURL);
			show("dgenEndpoint", dgenEndpoint);
			show("dgenFlowController", dgenFlowController);
			show("cmDescriptor", cmDescriptor);
			show("cmOverrides", cmOverrides);
			show("aeDescriptor", aeDescriptor);
			show("aeOverrides", aeOverrides);
			show("ccDescriptor", ccDescriptor);
			show("ccOverrides", ccOverrides);
			String targetDirectory = fabricateTargetDirectoryName(directory, id);
			AeGenerator aeGenerator = new AeGenerator(targetDirectory);
			ArrayList<IDuccUimaAggregateComponent> dgenComponents = new ArrayList<IDuccUimaAggregateComponent>();
			conditionalAddComponent(dgenComponents, cmDescriptor, cmOverrides);
			conditionalAddComponent(dgenComponents, aeDescriptor, aeOverrides);
			conditionalAddComponent(dgenComponents, ccDescriptor, ccOverrides);
			IDuccUimaDeployableConfiguration configuration = new DuccUimaAggregate(dgenName, dgenDescription, dgenThreadCount, dgenBrokerURL, dgenEndpoint, dgenFlowController, dgenComponents);
			retVal = aeGenerator.generate(configuration, id);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new AeException(e.toString());
		}
		return retVal;
	}

}
