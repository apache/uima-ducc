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
package org.apache.uima.ducc.cli.aio;

import java.io.IOException;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.ducc.cli.CliBase;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Level;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Toggle;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.resource.ResourceInitializationException;

public class AllInOne extends CliBase {
    
	private static String cid = AllInOne.class.getSimpleName();
	
	private static DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();
	
	private static XmiSerializationSharedData xmiSerializationSharedData = new XmiSerializationSharedData();
	
	private IMessageHandler mh = new MessageHandler();
	
	private JobRequestProperties jobRequestProperties = new JobRequestProperties(); 
	
	private boolean showStats = true;
	
	CasGenerator casGenerator;
	CasPipeline casPipeline;
	
	public AllInOne(String[] args) throws Exception {
		UiOption[] opts = DuccJobSubmit.opts;
		init(this.getClass().getName(), opts, args, jobRequestProperties, consoleCb);
	}
	
	private void examine_debug() {
		String mid = "examine_debug";
		debug = jobRequestProperties.containsKey(UiOption.Debug.pname());
		if(debug) {
			mh.setLevel(Level.FrameworkInfo, Toggle.On);
			mh.setLevel(Level.FrameworkDebug, Toggle.On);
			mh.setLevel(Level.FrameworkError, Toggle.On);
			mh.setLevel(Level.FrameworkWarn, Toggle.On);
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			String message = "false";
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_timestamp() {
		String mid = "examine_timestamp";
		boolean timestamp = jobRequestProperties.containsKey(UiOption.Timestamp.pname());
		if(timestamp) {
			mh.setTimestamping(Toggle.On);
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			String message = "false";
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine() throws IllegalArgumentException {
		String mid = "examine";
		mh.frameworkTrace(cid, mid, "enter");
		examine_debug();
		examine_timestamp();
		mh.frameworkTrace(cid, mid, "exit");
	}

	private class NoWorkItems extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	private void initialize() throws Exception {
		String mid = "initialize";
		mh.frameworkTrace(cid, mid, "enter");
		// Generator
		casGenerator = new CasGenerator(jobRequestProperties, mh);
		casGenerator.initialize();
		int total = casGenerator.getTotal();
		if(total > 0) {
			// Pipeline
			casPipeline = new CasPipeline(jobRequestProperties, mh);
			casPipeline.initialize();
		}
		else {
			throw new NoWorkItems();
		}
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void process() throws Exception {
		String mid = "process";
		mh.frameworkTrace(cid, mid, "enter");
		int count = 0;
		int total = casGenerator.getTotal();
		mh.frameworkDebug(cid, mid, "total:"+total);
		CAS cas = null;
		while(casGenerator.hasNext()) {
			cas = casGenerator.getCas(cas);
			mh.frameworkDebug(cid, mid, "cas:"+count);
			
			// Emulate a DUCC job by serializing then deserializing into the aggregate's possibly larger typesystem
			String serializedCas = uimaSerializer.serializeCasToXmi(cas, xmiSerializationSharedData);
			CAS cas2 = casPipeline.getEmptyCas();  // Always returns the same CAS
			uimaSerializer.deserializeCasFromXmi(serializedCas, cas2, xmiSerializationSharedData, true, -1);
			
			casPipeline.process(cas2);
			count++;
		}
		casPipeline.destroy();
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void statistics() {
		if(showStats) {
			casPipeline.dumpStatistics(System.out);
		}
	}
	
	public void go() throws Exception {
		String mid = "go";
		mh.frameworkTrace(cid, mid, "enter");
		try {
			examine();
			initialize();
			process();
			statistics();
		}
		catch(NoWorkItems e) {
			String message = "no work items";
			mh.warn(cid, mid, message);
		}
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	
	public static void main(String[] args) {
		try {
			AllInOne allInOne = new AllInOne(args);
			allInOne.go();
		} catch (Exception e) {
			// Indicate that something went wrong
			System.exit(1);
		}
	}

	@Override
	public boolean execute() throws Exception {
		return false;
	}

}
