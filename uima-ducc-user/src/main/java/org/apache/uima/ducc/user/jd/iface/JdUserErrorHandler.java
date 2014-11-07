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
package org.apache.uima.ducc.user.jd.iface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.ducc.user.jd.JdUser;

public class JdUserErrorHandler implements IJdUserErrorHandler {

	public enum InitializationDataKey { KillJobLimit, KillProcessLimit, KillWorkItemLimit };
	
	private static int DefaultJobErrorLimit = JdUser.DefaultJobErrorLimit;
	
	private AtomicInteger jobErrorLimit = new AtomicInteger(DefaultJobErrorLimit);
	
	private AtomicInteger jobErrorCount = new AtomicInteger(0);
	
	public JdUserErrorHandler() {
	}
	
	public JdUserErrorHandler(String initializationData) {
		initialize(initializationData);
	}
	
	private Map<String, String> parse(String initializationData) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			if(initializationData != null) {
				ArrayList<String> toks = QuotedOptions.tokenizeList(initializationData, true);
				if(toks != null) {
					for(String tok : toks) {
						String[] split = tok.split("=");
						String key = split[0].trim().toLowerCase();
						String value = split[1].trim();
						map.put(key, value);
					}
				}
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	@Override
	public void initialize(String initializationData) {
		if(initializationData != null) {
			Map<String, String> map = parse(initializationData);
			String key = InitializationDataKey.KillJobLimit.name().toLowerCase();
			String value = map.get(key);
			initKillJob(value);
		}
	}

	private void initKillJob(String value) {
		try {
			int expect = DefaultJobErrorLimit;
			int update = Integer.parseInt(value);
			jobErrorLimit.compareAndSet(expect, update);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public IJdUserDirective handle(CAS cas, Exception e) {
		JdUserDirective jdUserDirective = new JdUserDirective();
		jobErrorCount.incrementAndGet();
		if(jobErrorCount.get() > jobErrorLimit.get()) {
			jdUserDirective.setKillJob();
		}
		return jdUserDirective;
	}

}
