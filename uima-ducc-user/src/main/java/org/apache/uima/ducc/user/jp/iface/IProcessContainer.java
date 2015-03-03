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

package org.apache.uima.ducc.user.jp.iface;

import java.util.List;
import java.util.Properties;

public interface IProcessContainer {
	/**
	 * Configure the container for deployment. 
	 * 
	 * @param props - 
	 * @param args - command line arguments
	 * @return
	 * @throws Exception
	 */
	public int initialize(Properties props, String[] args) throws Exception;
	/**
	 * Deploy the container. 
	 * 
	 * @throws Exception
	 */
	public void deploy() throws Exception;
	/**
	 * Stop the container
	 * 
	 * @throws Exception
	 */
	public void stop() throws Exception;

	/**
	 * Return number of threads to use for processing
	 * 
	 * @return
	 */
	public int getScaleout();
	/**
	 * Analyze the CAS. 
	 * 
	 * @param xmi - serialized CAS in XMI format. Use org.apache.uima.ducc.user.common.DuccUimaSerializer 
	 * contained in ducc-user.jar to de-serialize XMI into a CAS if needed.
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<Properties> process(Object xmi) throws Exception;
	
	/**
	 * Return true if container requires thread affinity. The same thread that 
	 * called deploy() will also call process and stop. You may use
	 * this to attach data to ThreadLocal.
	 * 
	 * @return
	 */
	public boolean useThreadAffinity();
	
	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public String getKey(String cargo) throws Exception;
	
}
