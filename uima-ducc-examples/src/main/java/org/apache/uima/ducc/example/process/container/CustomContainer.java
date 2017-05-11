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
package org.apache.uima.ducc.example.process.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.user.jp.DuccAbstractProcessContainer;
/**
 * An example of Ducc custom process container that can be used
 * by a JP. To use custom container add the following jvm option:
 * 
 * -Dducc.deploy.JpProcessorClass=org.apache.uima.ducc.example.process.container.CustomContainer
 */
public class CustomContainer extends DuccAbstractProcessContainer {

	public int doInitialize(Properties p, String[] args) throws Exception {
		System.out.println("... Initializing Custom Process Container");
		super.sendStateUpdate(ProcessState.Initializing.name());
		synchronized( this ) {
			wait(10000); //simulate 10sec initialization time
		}
		super.sendStateUpdate(ProcessState.Running.name());
		return 2;    // scaleout
	}
	/**
	 * This method is called by the super class which switches
	 * class loader context to the Classloader that loaded jars
	 * from the -classpath
	 * 
	 */
	public void doStop() throws Exception {
		System.out.println("... Stopping Custom Process Container");
	}
	/**
	 * This method is called by the super class which switches
	 * class loader context to the Classloader that loaded jars
	 * from the -classpath
	 * 
	 */
	public List<Properties> doProcess(Object xmi) throws Exception {
		System.out.println("... Custom process() called");
		return new ArrayList<Properties>();
	}
	/**
	 * This method is called by the super class which switches
	 * class loader context to the Classloader that loaded jars
	 * from the -classpath
	 * 
	 */
	protected void doDeploy() throws Exception {
		System.out.println("... Deploying Custom Process Container");
	}
	public boolean useThreadAffinity() {
		return false;
	}

}
