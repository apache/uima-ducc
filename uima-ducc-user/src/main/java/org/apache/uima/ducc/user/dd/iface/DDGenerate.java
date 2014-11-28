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
package org.apache.uima.ducc.user.dd.iface;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.user.dd.DeploymentDescriptorGenerator;
import org.apache.uima.ducc.user.dd.DuccUimaAggregate;
import org.apache.uima.ducc.user.dd.DuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dd.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dd.IDuccUimaDeployableConfiguration;

public class DDGenerate implements IDDGenerate {

	public DDGenerate() {	
	}
	
	private void conditionalAddComponent(ArrayList<IDuccUimaAggregateComponent> ddComponents, String descriptor, List<String> overrides) {
		if(descriptor != null) {
			DuccUimaAggregateComponent ddComponent = new DuccUimaAggregateComponent(descriptor, overrides);
			ddComponents.add(ddComponent);
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
	
	@Override
	public String generate(
			String directory, 
			String id,
			String ddName,
			String ddDescription,
			Integer ddThreadCount,
			String ddBrokerURL,
			String ddEndpoint,
			String cmDescriptor,
			List<String> cmOverrides, 
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor,
			List<String> ccOverrides
			) throws DDException
	{
		String retVal = null;
		try {
			show("directory", directory);
			show("id", id);
			show("ddName", ddName);
			show("ddDescription", ddDescription);
			show("ddThreadCount", ddThreadCount.toString());
			show("ddBrokerURL", ddBrokerURL);
			show("ddEndpoint", ddEndpoint);
			show("cmDescriptor", cmDescriptor);
			show("cmOverrides", cmOverrides);
			show("aeDescriptor", aeDescriptor);
			show("aeOverrides", aeOverrides);
			show("ccDescriptor", ccDescriptor);
			show("ccOverrides", ccOverrides);
			DeploymentDescriptorGenerator ddg = new DeploymentDescriptorGenerator(directory);
			ArrayList<IDuccUimaAggregateComponent> ddComponents = new ArrayList<IDuccUimaAggregateComponent>();
			conditionalAddComponent(ddComponents, cmDescriptor, cmOverrides);
			conditionalAddComponent(ddComponents, aeDescriptor, aeOverrides);
			conditionalAddComponent(ddComponents, ccDescriptor, ccOverrides);
			IDuccUimaDeployableConfiguration configuration = new DuccUimaAggregate(ddName, ddDescription, ddThreadCount, ddBrokerURL, ddEndpoint, ddComponents);
			retVal = ddg.generate(configuration, id);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DDException(e.toString());
		}
		return retVal;
	}

}
