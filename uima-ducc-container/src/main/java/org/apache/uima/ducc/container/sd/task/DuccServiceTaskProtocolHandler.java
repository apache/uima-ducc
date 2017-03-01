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
import org.apache.uima.ducc.container.jd.fsm.wi.IMetaMetaCas;
import org.apache.uima.ducc.container.jd.fsm.wi.MetaMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Hint;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.apache.uima.ducc.container.sd.DuccServiceDriver;
import org.apache.uima.ducc.container.sd.iface.ServiceDriver;
import org.apache.uima.ducc.container.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.container.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.iface.TaskConsumer;
import org.apache.uima.ducc.container.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DuccServiceTaskProtocolHandler implements TaskProtocolHandler {
	Logger logger = UIMAFramework.getLogger(DuccServiceTaskProtocolHandler.class);

	private static AtomicInteger atomicCounter = 
			new AtomicInteger(0);
	public DuccServiceTaskProtocolHandler(TaskAllocatorCallbackListener taskAllocator) {
	}

	@Override
	public String initialize(Properties props) throws TaskProtocolException {
		return null;
	}

	@Override
	public void handle(IMetaCasTransaction wi) throws TaskProtocolException {
		handleMetaCasTransation(wi);
		
	}
	private void handleMetaCasTransation(IMetaCasTransaction trans) {
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
				logger.log(Level.INFO,"---- Driver handling GET Request - Requestor:"+taskConsumer.toString());
				handleMetaCasTransationGet(trans, taskConsumer);
				break;
			case Ack:
				logger.log(Level.INFO,"---- Driver handling ACK Request - Requestor:"+taskConsumer.toString());
				handleMetaCasTransationAck(trans, taskConsumer);
				break;
			case End:
				logger.log(Level.INFO,"---- Driver handling END Request - Requestor:"+taskConsumer.toString());
				handleMetaCasTransationEnd(trans, taskConsumer);
				break;
			case InvestmentReset:
			//	handleMetaCasTransationInvestmentReset(trans, rwt);
				break;
			default:
				break;
			}
			IMetaCas metaCas = trans.getMetaCas();
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}
	private void handleMetaCasTransationGet(IMetaCasTransaction trans, TaskConsumer taskConsumer) {
		IMetaMetaCas mmc = getMetaMetaCas(taskConsumer);
		trans.setMetaCas( mmc.getMetaCas());
	}
	private IMetaCas getMetaCas(String serializedCas) {
		if ( serializedCas == null ) {
			return null;
		}
		return new MetaCas(atomicCounter.incrementAndGet(), "", serializedCas);
	}
	private synchronized IMetaMetaCas getMetaMetaCas(TaskConsumer taskConsumer) {
		IMetaMetaCas mmc = new MetaMetaCas();
		ServiceDriver sd = DuccServiceDriver.getInstance();
		TaskAllocatorCallbackListener taskAllocator = 
				sd.getTaskAllocator();
				
		String serializedCas =
				taskAllocator.getSerializedCAS(taskConsumer);
		IMetaCas metaCas = getMetaCas(serializedCas);
		
		mmc.setMetaCas(metaCas);

		return mmc;
	}
	private void handleMetaCasTransationAck(IMetaCasTransaction trans, TaskConsumer taskConsumer) {

	}
	private Throwable deserialize(Object byteArray) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) byteArray);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Throwable t = (Throwable) ois.readObject();
		return t;
	}
	private void handleMetaCasTransationEnd(IMetaCasTransaction trans, TaskConsumer taskConsumer) {
		ServiceDriver sd = DuccServiceDriver.getInstance();
		TaskAllocatorCallbackListener taskAllocator = 
				sd.getTaskAllocator();
		if ( trans.getMetaCas().getUserSpaceException() != null ) {
			Object serializedException = 
					trans.getMetaCas().getUserSpaceException();
			String exceptionAsString="";
			try {
				Throwable t = deserialize(serializedException);
				StringWriter sw = new StringWriter();
				t.printStackTrace(new PrintWriter(sw));
				exceptionAsString = sw.toString();
			} catch( Exception ee) {
				logger.log(Level.WARNING,"Error",ee );
			}
			taskAllocator.onTaskFailure( taskConsumer, exceptionAsString );
			
		} else {
			IPerformanceMetrics m = 
					trans.getMetaCas().getPerformanceMetrics();
			
			taskAllocator.onTaskSuccess( taskConsumer, m );
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
