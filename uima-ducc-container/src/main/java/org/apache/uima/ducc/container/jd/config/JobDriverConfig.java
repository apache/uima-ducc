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
package org.apache.uima.ducc.container.jd.config;

public class JobDriverConfig implements IJobDriverConfig {

	private String[] userClasspath = null;
	private String crXml = null;
	private String crCfg = null;
	
	private String errorHandlerClassName = null;
	private String errorHandlerConfigurationParameters = null;
	
	@Override
	public void setUserClasspath(String[] value) {
		userClasspath = value;
	}

	@Override
	public String[] getUserClasspath() {
		return userClasspath;
	}

	@Override
	public void setCrXml(String value) {
		crXml = value;
	}

	@Override
	public String getCrXml() {
		return crXml;
	}

	@Override
	public void setCrCfg(String value) {
		crCfg = value;
	}

	@Override
	public String getCrCfg() {
		return crCfg;
	}

	@Override
	public void setErrorHandlerClassName(String value) {
		errorHandlerClassName = value;
	}

	@Override
	public String getErrorHandlerClassName() {
		return errorHandlerClassName;
	}

	@Override
	public void setErrorHandlerConfigurationParameters(String value) {
		errorHandlerConfigurationParameters = value;
	}

	@Override
	public String getErrorHandlerConfigurationParameters() {
		return errorHandlerConfigurationParameters;
	}

}
