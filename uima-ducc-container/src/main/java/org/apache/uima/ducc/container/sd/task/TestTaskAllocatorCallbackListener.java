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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.container.sd.task.iface.ITask;
import org.apache.uima.ducc.container.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.container.sd.task.iface.TaskConsumer;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.resource.metadata.impl.TypeSystemDescription_impl;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/*
 * Creates tasks/work from an input file or sends a fixed string every time.
 */
public class TestTaskAllocatorCallbackListener implements
		TaskAllocatorCallbackListener {
	Logger logger = UIMAFramework.getLogger(TestTaskAllocatorCallbackListener.class);
	private static DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();
	private AtomicLong seqno = new AtomicLong(0);
    private BufferedReader inf;
    private String text = "Some Text";
	public TestTaskAllocatorCallbackListener() throws FileNotFoundException {
	    String taskFilename = System.getProperty("TestTaskFile");
	    if (taskFilename != null) {
	        File taskFile = new File(taskFilename);
	        inf = new BufferedReader(new FileReader(taskFile));
	    }
	}
	private String serialize(CAS cas) throws Exception {
		String serializedCas = uimaSerializer.serializeCasToXmi(cas);
		return serializedCas;
	}
	public ITask getTask(TaskConsumer taskConsumer) {
		logger.log(Level.INFO,"getSerializedCAS() Call "+seqno.incrementAndGet()
		        + " - from "+taskConsumer.getType()+":"+taskConsumer.getHostName()+"-"+taskConsumer.getPid()+"-"+taskConsumer.getThreadId() );
		String serializedCas = null;
		try {
			CAS cas = null;
			cas = CasCreationUtils.createCas(new TypeSystemDescription_impl(), null, null);
			cas.setDocumentLanguage("en");
			if (inf != null) {
			    text = inf.readLine();
			    if (text == null) {
			        inf.close();
			        inf = null;
			    }
			}
			if (text == null) {
			    logger.log(Level.INFO," ... no work avaialble");
			    return null;
			}
			logger.log(Level.INFO,"delivering: " + text);
			cas.setDocumentText(text);
//			cas.setDocumentText("100 "+seqno.incrementAndGet()+" 1000 0");

			serializedCas = serialize(cas);
			cas.reset();
			cas.release();

		} catch( Exception e) {
			logger.log(Level.WARNING,"Error",e);
		}

		return new SimpleTask(serializedCas);
	}

	public synchronized void onTaskSuccess(TaskConsumer taskConsumer, String appdata, String processResult) {
		logger.log(Level.INFO,"onTaskSuccess() Starting");
		//List<Properties> breakdown = metrics.get();
		System.out.println("\tmetrics: "+processResult);
//		for( Properties p : breakdown ) {
//			StringBuffer sb = new StringBuffer();
//			sb.append("AE Name: ").append(p.get("uniqueName")).append(" Analysis Time: ").append(p.get("analysisTime"));
//			System.out.println("\tmetrics: "+taskConsumer.toString()+" -- "+sb.toString());
//		}
		logger.log(Level.INFO,"onTaskSuccess() Completed");
	}

	public void onTaskFailure(TaskConsumer taskConsumer, String appdata, String processError) {
		logger.log(Level.INFO,"onTaskFailure) Called");
	}

}
