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
package org.apache.uima.ducc.container.dgen;

import java.util.List;

import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.classloader.ProxyException;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.dgen.classload.ProxyDeployableGeneration;
import org.apache.uima.ducc.container.dgen.classload.ProxyDeployableGenerationException;
import org.apache.uima.ducc.user.common.QuotedOptions;

public class DgenManager {

	private static Logger logger = Logger.getLogger(DgenManager.class, IComponent.Id.JD.name());
	
	private static DgenManager instance = null;
	
	public synchronized static DgenManager getInstance() {
		String location = "getInstance";
		if(instance == null) {
			try {
				instance = new DgenManager();
			} catch (DgenException e) {
				logger.error(location, ILogger.null_id, e);
			}
		}
		return instance;
	}
	
	private String deployable = null;
	
	private ProxyDeployableGeneration proxy = null;
	
	public DgenManager() throws DgenException {
		initialize();
	}
	
	private void initialize() throws DgenException {
		FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
		initialize(
			feh.getJobDirectory(),
			feh.getJobId(),
			convert2Integer(feh.getJpThreadCount()),
			feh.getJpFlowController(),
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
			Integer dgenThreadCount,
			String flowController, 
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor, 
			List<String> ccOverrides,
			String cmDescriptor,
			List<String> cmOverrides,
			String referenceByName) throws DgenException {
		String location = "initialize";
		try {
			proxy = new ProxyDeployableGeneration();
			if(referenceByName == null) {
				String value = proxy.generate(jobDirectory, jobId, dgenThreadCount, flowController, cmDescriptor, cmOverrides, aeDescriptor, aeOverrides, ccDescriptor, ccOverrides);
				setDeployable(value);
				logger.info(location, null, "ae from parts: "+value);
			}
			else {
				String specification = referenceByName.trim();
				String value = proxy.generate(jobDirectory, jobId, dgenThreadCount, specification);
				setDeployable(value);
				logger.info(location, null, "dd generated: "+value);
			}
		}
		catch(ProxyDeployableGenerationException e) {
			logger.error(location, ILogger.null_id, e);
			throw new DgenException(e);
		}
		catch(ProxyException e) {
			logger.error(location, ILogger.null_id, e);
			throw new DgenException(e);
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
	
	public String getDeployable() {
		return deployable;
	}
	
	private void setDeployable(String value) {
		deployable = value;
	}
}
