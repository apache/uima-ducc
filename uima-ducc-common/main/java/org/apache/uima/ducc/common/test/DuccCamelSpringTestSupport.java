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
package org.apache.uima.ducc.common.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultClassResolver;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.exception.DuccComponentInitializationException;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.thoughtworks.xstream.XStream;
public abstract class DuccCamelSpringTestSupport extends CamelTestSupport  { //CamelSpringTestSupport {
	public static final String DUCC_PROPERTY_FILE="ducc.deploy.configuration";
	public static final String DUCC_DEPLOY_COMPONENTS="ducc.deploy.components";
	
	public String serialize(Object obj) throws Exception {
		XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
	        XStream xStream = xStreamDataFormat.getXStream(new DefaultClassResolver());
	        
	        return xStream.toXML(obj);
	}
	
	public void loadProperties(String componentProperties) throws Exception {
		DuccProperties duccProperties = new DuccProperties();
		duccProperties.load((String) System.getProperty(componentProperties));
		enrichSystemPropertiesWith(duccProperties);
		adjustEndpointsForSelectedTransport();
	}
	public void enrichSystemPropertiesWith(DuccProperties duccProperties ) {
		for( Map.Entry<Object,Object> entry : duccProperties.entrySet()) {
			if ( !System.getProperties().containsKey((String)entry.getKey()) ) {
				System.setProperty((String)entry.getKey(), (String)entry.getValue());
			} 
		}
	}
	private void adjustEndpointsForSelectedTransport() throws Exception {
		for( Map.Entry<Object,Object> entry : System.getProperties().entrySet()) {
			if ( ((String)entry.getKey()).endsWith("endpoint")) {
				String endpointValue = (String)entry.getValue();
				endpointValue = endpointValue.substring(endpointValue.lastIndexOf(":")+1, endpointValue.length());
				String endpointType = (String)System.getProperty((String)entry.getKey()+".type");
				
				if ( endpointType == null ) {
					throw new DuccComponentInitializationException("Endpoint type not specified in component properties. Specify vm, queue, or topic type value for endpoint: "+endpointValue);
				} else if ( endpointType.equals("vm") ) {
					if ( !endpointValue.startsWith("vm:")) {
						endpointValue = "vm:"+endpointValue;
						System.setProperty((String)entry.getKey(), endpointValue);
					} else {
						// ignore
					}
				} else if ( endpointType.equals("topic") ||  endpointType.equals("queue") ) {
					if ( !endpointValue.startsWith("activemq:"+endpointType)) {
						endpointValue = "activemq:"+endpointType+":"+endpointValue;
						System.setProperty((String)entry.getKey(), endpointValue);
					} else {
						// ignore
					}
				} else {
					throw new DuccComponentInitializationException("Provided Endpoint type is invalid:"+endpointType+". Specify vm, queue, or topic type value for endpoint: "+endpointValue);					
				}
			}
		}
	}
	/**
	 * Exits process if given configuration class is either null or not set (empty).
	 *  
	 * @param classToVerify - class name to check
	 * @param reason - 
	 */
	private void exitIfInvalid( String componentName, String classToVerify, String reason) {
		if ( classToVerify == null || classToVerify.trim().length() == 0) {
			System.out.println("Unable to start Component: "+componentName+". Missing "+reason+".\nUsage: java -DServiceConfigurationClass=<Configuration.class> ...");
			System.exit(-1);
		}
	}

	/**
	 * Extracts component configuration classes from System properties
	 * 
	 * @return - array of configuration classes
	 * 
	 * @throws DuccComponentInitializationException - if no components provided for loading
	 */
	private Class<?>[] getComponentsToLoad() throws Exception {
		String[] componentsToLoad = System.getProperty(DUCC_DEPLOY_COMPONENTS).split(",");
		if ( componentsToLoad == null || componentsToLoad.length == 0 ) {
			throw new DuccComponentInitializationException("Ducc Component not specified. Provide Ducc Component(s) to Load via -D"+DUCC_DEPLOY_COMPONENTS+" System property");
		}
		List<Class<?>> components = new ArrayList<Class<?>>(); 
		for( String componentToLoad : componentsToLoad ) {
			String configurationClassName = System.getProperty("ducc."+componentToLoad+".configuration.class");
			exitIfInvalid(componentToLoad, configurationClassName, "Configuration Class Name");
			components.add(Class.forName(configurationClassName));
		}
		Class<?>[] configClasses = new Class[components.size()];
		components.toArray(configClasses);
		return configClasses;
	}
	@Override
    public void setUp() throws Exception {
    	super.setUp();
    }

    protected AnnotationConfigApplicationContext createApplicationContext() { 
    	try {
    		loadProperties(DUCC_PROPERTY_FILE);
        	Class<?>[] configClasses = getComponentsToLoad();
            AnnotationConfigApplicationContext context = 
            	new AnnotationConfigApplicationContext(configClasses); 
    		Map<String,AbstractDuccComponent> duccComponents = 
    			context.getBeansOfType(AbstractDuccComponent.class);
    		//	Start all components
    		for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
    			System.out.println("... Starting Component:"+duccComponent.getKey());
    			duccComponent.getValue().start(null);
    		}

            return context;
    	} catch( Exception e) {
    		e.printStackTrace();
    	}
    	return null;
    }

}
