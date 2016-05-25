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
package org.apache.uima.ducc.transport.event.common;

import org.apache.uima.ducc.common.IDuccEnv;

public interface IDuccCommand {

	public String java = "java";
	public String main = "org.apache.uima.ducc.common.main.DuccService";

	public String arg_ducc_deploy_configruation = "-Dducc.deploy.configuration="+IDuccEnv.DUCC_HOME+"/resources/ducc.properties";
	
	public String arg_ducc_uima_as_deployment_descriptor = "-Dducc.uima-as.deployment.descriptor=";
	
	public String arg_ducc_deploy_components = "-Dducc.deploy.components=jd";
	
	public String arg_ducc_job_id = "-Dducc.job.id=";
	
}
