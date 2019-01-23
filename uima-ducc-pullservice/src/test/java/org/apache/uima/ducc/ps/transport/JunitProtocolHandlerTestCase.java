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
package org.apache.uima.ducc.ps.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.uima.ducc.ps.Client;
import org.apache.uima.ducc.ps.ServiceThreadFactory;
import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.protocol.IServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.protocol.builtin.DefaultNoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.protocol.builtin.DefaultServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.registry.DefaultRegistryClient;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;
import org.apache.uima.ducc.ps.service.transport.http.HttpServiceTransport;
import org.apache.uima.ducc.ps.service.transport.target.HttpTargetURI;
//import org.junit.Test;

public class JunitProtocolHandlerTestCase extends Client {

	CountDownLatch threadsReady;
	CountDownLatch stopLatch;
	
	private IServiceTransport initializeTransport() throws Exception {
		int scaleout = 1;
    	ITargetURI targetUrl = new HttpTargetURI("http://localhost:"+super.getPort()+"/"+super.getApp());
    	DefaultRegistryClient registryClient =
    			new DefaultRegistryClient(targetUrl);
    	INoTaskAvailableStrategy waitStrategy = 
				new DefaultNoTaskAvailableStrategy(1000);
    	HttpServiceTransport transport = 
    			new HttpServiceTransport(registryClient, scaleout,waitStrategy);
    	transport.initialize();
    	return transport;
	}
	   // TODO Currently this test hangs
	   //@Test
	    public void testProtocolHandlerBasicConnectivity() throws Exception
	    {
		   int scaleout = 2;
		   
		   threadsReady = new CountDownLatch(scaleout);
		   stopLatch = new CountDownLatch(scaleout);
		   
		   IServiceTransport transport =
				   initializeTransport();
			String analysisEngineDescriptor = "TestAAE";

		   UimaServiceProcessor processor =
				   new UimaServiceProcessor(analysisEngineDescriptor);
		   ServiceMockup service = 
				   new ServiceMockup(transport, processor, stopLatch);
		   
		   DefaultServiceProtocolHandler protocolHandler =
				   new DefaultServiceProtocolHandler.Builder()
				   .withProcessor(processor)
				   .withService(service)
				   .withTransport(transport)
				   .withDoneLatch(stopLatch)
				   .withInitCompleteLatch(threadsReady)
				   .build();
				   
		   service.setProtocolHandler(protocolHandler);
		   
		   service.initialize();

		   service.start();
		   
//	        assertThat("Response Code", http.getResponseCode(), (equal((HttpStatus.OK_200)));
	    }
	   
	   private class ServiceMockup implements IService {
		   private CountDownLatch stopLatch;
		   private IServiceTransport transport;
		   private IServiceProtocolHandler protocolHandler;
		   private IServiceProcessor processor;
		   private int scaleout = 2;
		   ScheduledThreadPoolExecutor threadPool;
		   
		   public ServiceMockup(IServiceTransport transport, IServiceProcessor processor, CountDownLatch stopLatch) {
			   this.transport = transport;
			  
			   this.processor = processor;
			   this.stopLatch = stopLatch;
		   }
		   public void setProtocolHandler( IServiceProtocolHandler protocolHandler) {
			   this.protocolHandler = protocolHandler;
		   }
		@Override
		public void start() throws ServiceException {
			try {
				stopLatch.await();
			} catch(InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			
		}

		@Override
		public void stop() {
			threadPool.shutdown();
			protocolHandler.stop();
			transport.stop(false);
			processor.stop();
		}
		@Override
		public void quiesceAndStop() {
			protocolHandler.quiesceAndStop();
			threadPool.shutdown();
			transport.stop(true);
			processor.stop();
		}
		@Override
		public void initialize() throws ServiceInitializationException {

			List<Future<String>> threadHandleList = new ArrayList<Future<String>>();
			threadPool = new ScheduledThreadPoolExecutor(scaleout, new ServiceThreadFactory());

			// Create and start worker threads that pull Work Items from a client
			for (int j = 0; j < scaleout; j++) {
				threadHandleList.add(threadPool.submit(protocolHandler));
			}
			try {
				// wait until all process threads initialize
				threadsReady.await();

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				threadPool.shutdownNow();
				throw new ServiceInitializationException(
						"Service interrupted during initialization - shutting down process threads");
			}
		}

		@Override
		public String getType() {
			// TODO Auto-generated method stub
			return null;
		}
		   
	   }
}
