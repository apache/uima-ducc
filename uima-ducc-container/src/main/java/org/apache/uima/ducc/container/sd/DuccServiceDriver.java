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

package org.apache.uima.ducc.container.sd;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.TransactionId;
import org.apache.uima.ducc.container.sd.iface.ServiceDriver;
import org.apache.uima.ducc.container.sd.task.DuccServiceTaskProtocolHandler;
import org.apache.uima.ducc.container.sd.task.TestTaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.container.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.ducc.container.sd.task.transport.TaskTransportException;
import org.apache.uima.ducc.container.sd.task.transport.Transports;
import org.apache.uima.ducc.container.sd.task.transport.Transports.TransportType;
import org.apache.uima.ducc.container.sd.task.transport.iface.TaskTransportHandler;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DuccServiceDriver implements ServiceDriver {
	Logger logger = UIMAFramework.getLogger(DuccServiceDriver.class);
	
	private TaskAllocatorCallbackListener taskAllocator;
	private TaskTransportHandler transport;
	private TransportType transportType = TransportType.HTTP;
	private TaskProtocolHandler protocolHandler = null;
	
	// prevent empty constructor instantiation 
	private DuccServiceDriver() {		
	}
	/**
	 * Thread safe singleton helper class
	 */
	private static class DuccServiceDriverSingleton {
		private static final DuccServiceDriver instance =
				new DuccServiceDriver();
	}
	public static DuccServiceDriver getInstance() {
		return DuccServiceDriverSingleton.instance;
	}
	public TaskAllocatorCallbackListener getTaskAllocator() {
		return taskAllocator;
	}
	public void setTaskAllocator(TaskAllocatorCallbackListener taskAllocator) {
		this.taskAllocator = taskAllocator;
	}
	public String start() throws Exception {
		if ( protocolHandler == null ) {
			throw new DriverException("start() called before initialize()");
		}
		if ( transport == null ) {
			throw new DriverException("start() called before initialize()");
		}
		if ( taskAllocator == null ) {
			throw new DriverException("start() called before setTaskAllocator()");
		}
		String retValue = protocolHandler.start();
		transport.start();
		
		return retValue;
	}
	public void stop() throws Exception {
		if ( transport != null ) {
			transport.stop();
		}
		if ( protocolHandler != null ) {
			protocolHandler.stop();
		}
	}
	public void initialize(Properties properties) throws DriverException {
		if ( TransportType.HTTP.equals(transportType)) {
			transport = 
					Transports.newHttpTransport();
			
			protocolHandler = new DuccServiceTaskProtocolHandler(taskAllocator);
			try {
				
				protocolHandler.initialize(properties);
				logger.log(Level.INFO, "... Protocol handler initialized ...");
				transport.setTaskProtocolHandler(protocolHandler);
				transport.initialize(properties);
				
				start();
			
			} catch( TaskProtocolException e) {
				throw new DriverException(e);
			} catch( TaskTransportException e) {
				throw new DriverException(e);
			} catch( Exception e) {
				throw new DriverException(e);
			}
		}
	}
	public void test() throws Exception {
		AtomicInteger IdGenerator =
	    		new AtomicInteger();
	    IMetaCasTransaction transaction = new MetaCasTransaction();
	
		int major = IdGenerator.addAndGet(1);
		int minor = 0;
	
		TransactionId tid = new TransactionId(major, minor);
		transaction.setTransactionId(tid);
		// According to HTTP spec, GET may not contain Body in 
		// HTTP request. HttpClient actually enforces this. So
		// do a POST instead of a GET.
		transaction.setType(Type.Get);  // Tell JD you want a Work Item
		protocolHandler.handle(transaction);
		logger.log(Level.INFO,"Returned from Get Handler - Client Got Message");
		transaction.setType(Type.Ack);  // Tell JD you want a Work Item
		protocolHandler.handle(transaction);
		logger.log(Level.INFO,"Returned from Ack Handler - Client Got Message");
		transaction.setType(Type.End);  // Tell JD you want a Work Item
		protocolHandler.handle(transaction);
		logger.log(Level.INFO,"Returned from End Handler - Client Got Message");

	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Properties properties = new Properties();
			properties.put(ServiceDriver.Port, "8888");
			properties.put(ServiceDriver.MaxThreads, "100");
			properties.put(ServiceDriver.Application, "/test");
			
			TaskAllocatorCallbackListener taskAllocator =
					new TestTaskAllocatorCallbackListener();
			ServiceDriver driver = DuccServiceDriver.getInstance();
			driver.setTaskAllocator(taskAllocator);
			driver.initialize(properties);
			//driver.start();
		//	((DuccServiceDriver)driver).test();
				
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}
