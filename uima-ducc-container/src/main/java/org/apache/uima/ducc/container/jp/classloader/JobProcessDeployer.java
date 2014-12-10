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

package org.apache.uima.ducc.container.jp.classloader;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.uima.ducc.container.common.classloader.PrivateClassLoader;
import org.apache.uima.ducc.container.jp.UimaProcessor;
import org.apache.uima.ducc.container.jp.iface.IJobProcessDeployer;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.container.jp.iface.ServiceFailedInitialization;

public class JobProcessDeployer implements IJobProcessDeployer {
	// declare methods to be called via reflection
	private static String M_DEPLOY="deploy";
	private static String M_PROCESS="process";
	private static String M_STOP="stop";
	private static String M_INITIALIZE="initialize";
	
    private boolean DEBUG = false;
    Method processMethod = null;
    Method stopMethod = null;
    Method deployMethod = null;
    Object uimaContainerInstance = null;
    int scaleout=1;
    
    
    public int initialize(String userClasspath, String[] args, String clzToLoad) throws ServiceFailedInitialization {
		try {

			URLClassLoader ucl = PrivateClassLoader.create(userClasspath);
			// This is needed to launch ActiveMQ 
			Thread.currentThread().setContextClassLoader(ucl);
			
			Class<?> classToLaunch = ucl.loadClass(clzToLoad);

			if( DEBUG ) {
				URL[] urls2 = ucl.getURLs();
				for( URL u : urls2 ) {
					System.out.println("-----------:"+u.getFile());
				}
			}
			Method initMethod = classToLaunch.getMethod(M_INITIALIZE, String[].class);
			processMethod = classToLaunch.getMethod(M_PROCESS, Object.class);
			stopMethod = classToLaunch.getMethod(M_STOP);
			deployMethod = classToLaunch.getMethod(M_DEPLOY);

			uimaContainerInstance = classToLaunch.newInstance();
			Object s = initMethod.invoke(uimaContainerInstance,
					(Object) args);
	        this.scaleout = (Integer)s;
	        
			return scaleout;
			
		} catch( Exception e) {
			throw new ServiceFailedInitialization(e);
		}

    	
    }
    public IUimaProcessor deploy() throws ServiceFailedInitialization {
		try {
	    	// This blocks until Uima AS container is fully initialized
			deployMethod.invoke(uimaContainerInstance);
	    	return new UimaProcessor(uimaContainerInstance,processMethod,stopMethod,scaleout);
			
		} catch( Exception e) {
			throw new ServiceFailedInitialization(e);
		}
	}
	
}
