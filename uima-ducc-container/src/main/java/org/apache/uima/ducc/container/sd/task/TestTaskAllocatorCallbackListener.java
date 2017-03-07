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

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.container.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.iface.TaskConsumer;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.resource.metadata.impl.TypeSystemDescription_impl;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class TestTaskAllocatorCallbackListener implements
		TaskAllocatorCallbackListener {
	Logger logger = UIMAFramework.getLogger(TestTaskAllocatorCallbackListener.class);
	private static DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();
	private AtomicLong seqno = new AtomicLong(0);
	public TestTaskAllocatorCallbackListener() {
	}
	public String serialize(CAS cas) throws Exception {
		String serializedCas = uimaSerializer.serializeCasToXmi(cas);
		return serializedCas;
	}
	public String getSerializedCAS(TaskConsumer taskConsumer) {
		logger.log(Level.INFO,"getSerializedCAS() Called "+seqno.incrementAndGet()+ " Times");
		String serializedCas = null;
		try {
			CAS cas = null;
			cas = CasCreationUtils.createCas(new TypeSystemDescription_impl(), null, null);
			cas.setDocumentLanguage("en");
			cas.setDocumentText("Some Text");	
//			cas.setDocumentText("100 "+seqno.incrementAndGet()+" 1000 0");	
			
			serializedCas = serialize(cas);
			cas.reset();
			cas.release();
			
		} catch( Exception e) {
			logger.log(Level.WARNING,"Error",e);
		}
		
		return serializedCas;
	}

	public synchronized void onTaskSuccess(TaskConsumer taskConsumer, IPerformanceMetrics metrics) {
		logger.log(Level.INFO,"onTaskSuccess() Called");
		List<Properties> breakdown = metrics.get();
		
		for( Properties p : breakdown ) {
			StringBuffer sb = new StringBuffer();
			sb.append("AE Name: ").append(p.get("uniqueName")).append(" Analysis Time: ").append(p.get("analysisTime"));
			System.out.println(taskConsumer.toString()+" -- "+sb.toString());
		}
	}

	public void onTaskFailure(TaskConsumer taskConsumer, String stringifiedException) {
		logger.log(Level.INFO,"onTaskFailure) Called");
	}

}
