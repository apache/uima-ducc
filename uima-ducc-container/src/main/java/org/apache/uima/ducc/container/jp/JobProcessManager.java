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
package org.apache.uima.ducc.container.jp;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.container.jp.classloader.JobProcessDeployer;
import org.apache.uima.ducc.container.jp.iface.IJobProcessDeployer;
import org.apache.uima.ducc.container.jp.iface.IJobProcessManager;
import org.apache.uima.ducc.container.jp.iface.IJobProcessManagerCallbackListener;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.container.jp.iface.ServiceFailedInitialization;


public class JobProcessManager implements IJobProcessManager {
	private IJobProcessManagerCallbackListener callbackListener;
	private IJobProcessDeployer jobProcessDeployer;
	List<IUimaProcessor> processors = new ArrayList<IUimaProcessor>();
	private IUimaProcessor processor = null;

	public JobProcessManager() {
		jobProcessDeployer = new JobProcessDeployer();
	}
	public int initialize(String userClasspath, String[] args, String clz) throws ServiceFailedInitialization {
		return jobProcessDeployer.initialize(userClasspath, args, clz);
	}
	
	public void setCallbackListener(IJobProcessManagerCallbackListener callbackListener ) {
		this.callbackListener = callbackListener;
	}
	
	public IJobProcessManagerCallbackListener setCallbackListener() {
		return this.callbackListener;
	}
	
	public synchronized IUimaProcessor deploy() throws ServiceFailedInitialization {
//		String jobType = System.getProperty(FlagsHelper.Name.JpType.pname());
//		if ( jobType.trim().equals("uima-as") ) {
//			if ( processors.size() == 0) {
//				// This blocks until the UIMA AS service is deployed and initialized
//   			    processor = jobProcessDeployer.deploy();
//				processors.add(processor);
//			}
//			return processor;
//		}
        processor = jobProcessDeployer.deploy();
		processors.add(processor);
		return processor;
	}

    public void stop() throws Exception {
    	for( IUimaProcessor processor : processors ) {
    		processor.stop();
    	}
    }

}
