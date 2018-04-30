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
package org.apache.uima.ducc.ps.service.builders;

import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.main.PullService;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;

public final class PullServiceStepBuilder {
	private PullServiceStepBuilder() {}

	public static ServiceProcessorStep newBuilder() {
		return new ServiceSteps();
	}
	private static class ServiceSteps implements ServiceProcessorStep, RegistryStep, OptionalsStep, BuildStep {
		private IServiceProcessor serviceProcessor;
		private IRegistryClient registryClient;
		
		// uniqueId is provided by an agent when launching this process. Must be
		// echoed when reporting process state
		//private String uniqueId;
		private int scaleout = 1;
		private String serviceType;
		private String clientURL;
		
		@Override
		public IService build() {
			
			PullService service = new PullService(serviceType);
			service.setScaleout(scaleout);
			if ( registryClient == null ) {
				service.setClientURL(clientURL);	
			} else {
				service.setRegistryClient(registryClient);	
			}

			service.setServiceProcessor(serviceProcessor);

			return service;
		}
	
		@Override
		public OptionalsStep withScaleout(int scaleout) {
			this.scaleout = scaleout;
			return this;
		}
		@Override
		public OptionalsStep withType(String type) {
			this.serviceType = type;
			return this;
		}
		@Override
		public BuildStep withOptionalsDone() {
			return this;
		}
		@Override
		public OptionalsStep withRegistry(IRegistryClient registry) {
			this.registryClient = registry;
			return this;
		}
		@Override
		public OptionalsStep withClientURL(String clientURL) {
			this.clientURL = clientURL;
			return this;
		}
		@Override
		public RegistryStep withProcessor(IServiceProcessor processor) {
			this.serviceProcessor = processor;
			return this;
		}
		
	}
	
	public interface ServiceProcessorStep {
		public RegistryStep withProcessor(IServiceProcessor processor);
	}
	public interface RegistryStep {
		public OptionalsStep withRegistry(IRegistryClient registry);
		public OptionalsStep withClientURL(String clientURL);
	}
	public interface OptionalsStep {
		public OptionalsStep withScaleout(int scaleout);
		public OptionalsStep withType(String type);
		
		public BuildStep withOptionalsDone();
	}
	public interface BuildStep {
		IService build();
	}
	public static void main(String[] args) {
//		// the actual URL may be provided by a Registry.
//		ITargetURI clientTarget = 
//				new HttpTargetURI("http://localhost:8080/TAS");
//		ITargetURI monitorTarget = 
//				new SocketTargetURI("tcp://localhost:12000");
		int scaleout = 1;
		String analysisEngineDescriptor = args[0];
		
		IServiceProcessor processor =
				new UimaServiceProcessor(analysisEngineDescriptor);
		
		IService service = PullServiceStepBuilder.newBuilder()
			.withProcessor(processor)
			.withClientURL("http://localhost:8080/TAS")
			.withType("Note Service")
			.withScaleout(scaleout)
			.withOptionalsDone()
			.build();
		
		try {
			service.initialize();
			service.start();
		} catch(ServiceInitializationException e) {
			
		} catch(ServiceException e)	{
			
		}
	}

}
