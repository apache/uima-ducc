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

package org.apache.uima.ducc.container.sd.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;

import org.apache.uima.ducc.container.sd.DuccServiceDriver;
import org.apache.uima.ducc.container.sd.iface.ServiceDriver;
import org.apache.uima.ducc.container.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.container.sd.task.iface.ITask;
import org.apache.uima.ducc.container.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.iface.TaskConsumer;
import org.apache.uima.ducc.container.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.ducc.ps.net.iface.IMetaMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Hint;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.impl.MetaMetaTask;
import org.apache.uima.ducc.ps.net.impl.MetaTask;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DuccServiceTaskProtocolHandler implements TaskProtocolHandler {
	Logger logger = UIMAFramework.getLogger(DuccServiceTaskProtocolHandler.class);
	private volatile boolean running = true;;
	private final long secondsToWait = 30;
	private static AtomicInteger atomicCounter = 
			new AtomicInteger(0);
	public DuccServiceTaskProtocolHandler(TaskAllocatorCallbackListener taskAllocator) {
	}

	@Override
	public String initialize(Properties props) throws TaskProtocolException {
		return null;
	}

	@Override
	public void handle(IMetaTaskTransaction wi) throws TaskProtocolException {
		handleMetaTaskTransation(wi);
		
	}
	private void handleMetaTaskTransation(IMetaTaskTransaction trans) {
		try {
			trans.setResponseHints(new ArrayList<Hint>());

			TaskConsumer taskConsumer = 
					new WiTaskConsumer(trans);

			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+taskConsumer.toString());
			mb.append(Standardize.Label.type.get()+trans.getType());
			Type type = trans.getType();
			switch(type) {
			case Get:
				logger.log(Level.FINE,"---- Driver handling GET Request - Requestor:"+taskConsumer.toString());
				handleMetaTaskTransationGet(trans, taskConsumer);
				break;
			case Ack:
				logger.log(Level.FINE,"---- Driver handling ACK Request - Requestor:"+taskConsumer.toString());
				handleMetaTaskTransationAck(trans, taskConsumer);
				break;
			case End:
				logger.log(Level.FINE,"---- Driver handling END Request - Requestor:"+taskConsumer.toString());
				handleMetaTaskTransationEnd(trans, taskConsumer);
				break;
			case InvestmentReset:
			//	handleMetaCasTransationInvestmentReset(trans, rwt);
				break;
			default:
				break;
			}
			IMetaTask metaCas = trans.getMetaTask();
			if(metaCas != null) {
				metaCas.setPerformanceMetrics(null);
				metaCas.setUserSpaceException(null);
			}
		}
		catch(Exception e) {
			logger.log(Level.WARNING,"Error",e);
		}
		finally {
		}
	}

	@Override
	public String start() throws Exception {
		running = true;
		return null;
	}

	@Override
	public void stop() throws Exception {
		running = false;
	}
	private void handleMetaTaskTransationGet(IMetaTaskTransaction trans, TaskConsumer taskConsumer) {
		IMetaMetaTask mmc = getMetaMetaTask(taskConsumer);
		trans.setMetaTask( mmc.getMetaCas());
	}
	private IMetaTask getMetaTask(String serializedCas) {
		if ( serializedCas == null ) {
			return null;
		}
		return new MetaTask(atomicCounter.incrementAndGet(), "", serializedCas);
	}
	private synchronized IMetaMetaTask getMetaMetaTask(TaskConsumer taskConsumer) {
		IMetaMetaTask mmc = new MetaMetaTask();
		ServiceDriver sd = DuccServiceDriver.getInstance();
		TaskAllocatorCallbackListener taskAllocator = 
				sd.getTaskAllocator();
		ITask task;
		// The max time we are willing to wait for a task is 60 secs
		// with 2 secs wait time between retries. With the above
		// the max number of retries is 30. When we reach the max
		// retry, we return no work to the service.
		long retryCount = 60/secondsToWait;
		while( retryCount > 0 ) {
			task = taskAllocator.getTask(taskConsumer);
			// if allocation system does not return a task (or empty)
			// block this thread and retry until a task becomes
			// available or until max retry count is exhausted
			if ( task == null || task.isEmpty() ) {
				try {
					this.wait(secondsToWait*1000);
				} catch(InterruptedException ee) {
					Thread.currentThread().interrupt();
				}
			} else {
				IMetaTask metaTask = getMetaTask(task.asString());
				mmc.setMetaCas(metaTask);
				break;
			}
			retryCount--;
		}

		return mmc;
	}
	private void handleMetaTaskTransationAck(IMetaTaskTransaction trans, TaskConsumer taskConsumer) {

	}
	private Throwable deserialize(Object byteArray) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) byteArray);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Throwable t = (Throwable) ois.readObject();
		return t;
	}
	private void handleMetaTaskTransationEnd(IMetaTaskTransaction trans, TaskConsumer taskConsumer) {
		ServiceDriver sd = DuccServiceDriver.getInstance();
		TaskAllocatorCallbackListener taskAllocator = 
				sd.getTaskAllocator();
		if ( trans.getMetaTask().getUserSpaceException() != null ) {
			Object serializedException = 
					trans.getMetaTask().getUserSpaceException();
			String exceptionAsString="";
			try {
				Throwable t = deserialize(serializedException);
				StringWriter sw = new StringWriter();
				t.printStackTrace(new PrintWriter(sw));
				exceptionAsString = sw.toString();
			} catch( Exception ee) {
				logger.log(Level.WARNING,"Error",ee );
			}
			taskAllocator.onTaskFailure( taskConsumer, trans.getMetaTask().getAppData(), exceptionAsString );
			
		} else {
			String m = 
					trans.getMetaTask().getPerformanceMetrics();
			
			taskAllocator.onTaskSuccess( taskConsumer,trans.getMetaTask().getAppData(), m );
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
