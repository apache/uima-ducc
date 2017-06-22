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
package org.apache.uima.ducc.user.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.adapter.jms.activemq.SpringContainerDeployer;
import org.apache.uima.adapter.jms.service.UIMA_Service;
import org.apache.uima.ducc.user.jp.DuccAbstractProcessContainer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
/**
 * This class is used to deploy DUCC UIMA-AS based services
 *
 */
public class UimaASServiceContainer  extends DuccAbstractProcessContainer 
implements ApplicationListener<ApplicationEvent> {
    // command line args   
	private String[] args = null;
    // UIMA-AS service deployer
	private SpringContainerDeployer serviceDeployer;
	
	public volatile boolean initialized = false;
	/**
	 * When true, DUCC will call deploy, process, and stop using the same
	 * thread. Otherwise, no such guarantee is provided.
	 * 
	 */
	public boolean useThreadAffinity() {
		return false;
	}
	/**
	 * Generates Spring context file from provided deployment descriptor 
	 * and deploys UIMA-AS service from it. Once deployed, the UIMA-AS
	 * service begins listening on its JMS queue and processes work. 
	 * 
	 */
	protected void doDeploy() throws Exception {
		UIMA_Service service = new UIMA_Service();
		if ( args == null || args.length == 0 ) {
			throw new RuntimeException("Unable to Deploy UIMA-AS service Due to Missing Deployment Descriptor ");
		}

		// HACK - UIMA-AS should load DDs from the classpath or datapath
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("-dd") && !args[i+1].endsWith(".xml")) {
        args[i+1] = copyDD(args[i+1]);
      }
    }

    // parse command args and run dd2spring to generate spring context
    // files from deployment descriptors
		String[] contextFiles = service.initialize(args);
		if (contextFiles == null) {
			throw new Exception(
					"Spring Context Files Not Generated. Unable to Launch Uima AS Service");
		}

		// Deploy components defined in Spring context files.
		// !!!! NOTE:This method blocks until the container is fully
		// initialized and all UIMA-AS components are successfully deployed
		// or there is a failure.
		serviceDeployer = service.deploy(contextFiles, this);

		if (serviceDeployer == null || serviceDeployer.initializationFailed()) {
			System.out
					.println(">>> Failed to Deploy UIMA Service");
		    throw new ResourceInitializationException(new RuntimeException("Unable to deploy UIMA-AS service from "+args[0]));
		} else {
			System.out
			.println(">>> Service Container Deployed Successfully");
		}
	}

	private static final Class<?> CLASS_NAME = UimaASServiceContainer.class;
	
	// Load DD from classpath and create file copy to satisfy dd2spring 
	private String copyDD(String resourceName) throws IOException {
	  String resourceFile = resourceName.replace('.', '/') + ".xml";
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceFile);
    if (is == null) {
      return resourceName;
    }
	  String logDir = System.getProperty("ducc.process.log.dir");
	  File dir = new File(logDir);
	  if ( !dir.exists()) {
       dir.mkdirs();
	  }
    File outf = File.createTempFile("uima-as-dd-", ".xml", dir);
    try (FileOutputStream outs = new FileOutputStream(outf)) {
      byte[] buf = new byte[4096];
      int len;
      while ((len = is.read(buf)) > 0) {
        outs.write(buf, 0, len);
      }
    }
    UIMAFramework.getLogger(CLASS_NAME).log(Level.INFO, "Copied resource "+resourceName+" as "+outf.getAbsolutePath());
	  return outf.getAbsolutePath();
	}
	  
	protected int doInitialize(Properties p, String[] args) throws Exception {
		this.args = args;
		return 1;  // default scaleout of 1
	}
	
	protected void doStop() throws Exception {
		if (serviceDeployer != null) {
			// Use top level controller to stop all components
			serviceDeployer.getTopLevelController().stop();
		}
	}
	/**
	 * This is just a stub and will not be called as UIMA-AS based
	 * service takes work items from its JMS queue directly.
	 * 
	 */
	protected List<Properties> doProcess(Object subject) throws Exception {
		return null;
	}

	/**
	 * This is a callback method called by UIMA-AS during initialization
	 */
	public void onApplicationEvent(ApplicationEvent arg0) {
		// TODO Auto-generated method stub
		
	}

}
