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
*/package org.apache.uima.ducc.container.dd;

import java.util.List;

import org.apache.uima.ducc.common.utils.QuotedOptions;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.dd.classload.ProxyDDException;
import org.apache.uima.ducc.container.dd.classload.ProxyDDGenerate;

public class DdManager {

	private static Logger logger = Logger.getLogger(DdManager.class, IComponent.Id.JD.name());

	private String dd = null;
	
	private ProxyDDGenerate pddg = null;
	
	public DdManager() throws DdException {
		initialize();
	}
	
	private void initialize() throws DdException {
		FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
		initialize(
			feh.getJobDirectory(),
			feh.getJobId(),
			feh.getJpDdName(),
			feh.getJpDdDescription(),
			convert2Integer(feh.getJpDdThreadCount()),
			feh.getJpDdBrokerURL(),
			feh.getJpDdBrokerEndpoint(),
			feh.getJpAeDescriptor(), 
			convert2List(feh.getJpAeOverrides()), 
			feh.getJpCcDescriptor(), 
			convert2List(feh.getJpCcOverrides()),
			feh.getJpCmDescriptor(),
			convert2List(feh.getJpCmOverrides()),
			feh.getJpDd());
	}
	
	public void initialize(
			String jobDirectory,
			String jobId,
			String ddName,
			String ddDescription,
			Integer ddThreadCount,
			String ddBrokerURL,
			String ddBrokerEndpoint,
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor, 
			List<String> ccOverrides,
			String cmDescriptor,
			List<String> cmOverrides,
			String dd) throws DdException {
		String location = "initialize";
		try {
			if(dd == null) {
				pddg = new ProxyDDGenerate();
				String value = pddg.generate(jobDirectory, jobId, ddName, ddDescription, ddThreadCount, ddBrokerURL, ddBrokerEndpoint, cmDescriptor, cmOverrides, aeDescriptor, aeOverrides, ccDescriptor, ccOverrides);
				setDD(value);
				logger.info(location, null, "generated dd: "+value);
			}
			else {
				setDD(dd);
				logger.info(location, null, "specified dd: "+dd);
			}
			
		}
		catch(ProxyDDException e) {
			logger.error(location, ILogger.null_id, e);
			throw new DdException(e);
		}
	}
	
	private List<String> convert2List(String input) {
		List<String> list = null;
		if(input != null) {
			list = QuotedOptions.tokenizeList(input, true);
		}
		return list;
	}
	
	private Integer convert2Integer(String input) {
		Integer retVal = null;
		if(input == null) {
			retVal = new Integer(1);
		}
		else {
			retVal = Integer.parseInt(input);
		}
		return retVal;
	}
	
	public String getDD() {
		return dd;
	}
	
	private void setDD(String value) {
		dd = value;
	}
}
