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

package org.apache.uima.ducc.transport.configuration.jd;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.container.FlagsHelper.Name;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.mh.IMessageHandler;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.transport.configuration.jd.iface.IJobDriverComponent;

public class JobDriverComponent extends AbstractDuccComponent
implements IJobDriverComponent {
	
	private static DuccLogger logger = DuccLoggerComponents.getJdOut(JobDriverComponent.class.getName());
	private static DuccId jobid = null;
	private static String node = null;
	private static int port = 0;
	
	private static JobDriverStateExchanger jdse = null;
	
	private JobDriverConfiguration configuration;
	
	public JobDriverComponent(String componentName, CamelContext ctx, JobDriverConfiguration jdc) {
		super(componentName,ctx);
		this.configuration = jdc;
		verifySystemProperties();
		jdse = new JobDriverStateExchanger();
		jdse.setJobDriverComponent(this);
		jdse.start();
		createInstance();
	}
	
	private void verifySystemProperties() {
		String location = "verifySystemProperties";
		Properties properties = System.getProperties();
		ArrayList<String> missing = new ArrayList<String>();
		for(Name name : FlagsHelper.Name.values()) {
			String key = name.pname();
			if(properties.containsKey(key)) {
				String value = properties.getProperty(key);
				String text = key+"="+value;
				logger.info(location, jobid, text);
			}
			else {
				if(name.isRequiredJd()) {
					missing.add(name.name());
					String text = key+" is missing.";
					logger.error(location, jobid, text);
				}
			}
		}
		if(missing.size() > 0) {
			throw new RuntimeException("Missing System Properties: "+missing.toString());
		}
	}
	
	private void createInstance() {
		String location = "createInstance";
		try {
			int total = JobDriver.getInstance().getCasManager().getCasManagerStats().getCrTotal();
			logger.info(location, jobid, "total: "+total);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			throw new RuntimeException(e);
		}
	}
	
	public JobDriverConfiguration getJobDriverConfiguration() {
		return configuration;
	}
	
	public String getJmxUrl() {
		return getProcessJmxUrl();
	}
	
	public String getNode() {
		return node;
	}
	
	public void setNode(String value) {
		node = value;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int value) {
		port = value;
	}
	
	@Override
	public DuccLogger getLogger() {
		return logger;
	}
	
	public void handleJpRequest(IMetaCasTransaction metaCasTransaction) throws Exception {
		String location = "handleJpRequest";
		try {
			IMessageHandler mh = JobDriver.getInstance().getMessageHandler();
			mh.handleMetaCasTransation(metaCasTransaction);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			throw e;
		}
	}

}
