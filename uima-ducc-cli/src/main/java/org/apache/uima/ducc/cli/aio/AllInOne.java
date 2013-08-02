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

import org.apache.commons.cli.MissingArgumentException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.ducc.cli.CliBase;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Level;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Toggle;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.resource.ResourceInitializationException;

public class AllInOne extends CliBase {
    
	private static String cid = AllInOne.class.getSimpleName();
	
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
	
	private void examine() throws MissingArgumentException, IllegalArgumentException {
		String mid = "examine";
		mh.frameworkTrace(cid, mid, "enter");
		examine_debug();
		examine_timestamp();
		mh.frameworkTrace(cid, mid, "exit");
	}

	private void initialize() throws Exception {
		String mid = "initialize";
		mh.frameworkTrace(cid, mid, "enter");
		// Generator
		casGenerator = new CasGenerator(jobRequestProperties, mh);
		casGenerator.initialize();
		// Pipeline
		casPipeline = new CasPipeline(jobRequestProperties, mh);
		casPipeline.initialize();
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void process() throws CollectionException, ResourceInitializationException, IOException, AnalysisEngineProcessException {
		String mid = "process";
		mh.frameworkTrace(cid, mid, "enter");
		int count = 0;
		int total = casGenerator.getTotal();
		mh.frameworkDebug(cid, mid, "total:"+total);
		CAS cas = null;
		while(casGenerator.hasNext()) {
			cas = casGenerator.getCas(cas);
			mh.frameworkDebug(cid, mid, "cas:"+count);
			casPipeline.process(cas);
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
		examine();
		initialize();
		process();
		statistics();
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	
	public static void main(String[] args) throws Exception {
		AllInOne allInOne = new AllInOne(args);
		allInOne.go();
	}

	@Override
	public boolean execute() throws Exception {
		return false;
	}

}
