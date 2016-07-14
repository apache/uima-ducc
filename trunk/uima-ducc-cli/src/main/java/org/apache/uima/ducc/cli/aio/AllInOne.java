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

import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;

public class AllInOne {
    
	private static String cid = AllInOne.class.getSimpleName();
	
	private static DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();
	
    private MsgHandler mh = new MsgHandler();
    
    private Properties jobRequestProperties = new Properties();

    private boolean showStats = true;

	// Avoid a dependency on the CLI's UiOptions by duplicating these option nanes
    static String DriverDescriptorCR = "driver_descriptor_CR";
    static String DriverDescriptorCROverrides = "driver_descriptor_CR_overrides";
    static String ProcessDD = "process_descriptor_DD";
    static String ProcessDescriptorCM = "process_descriptor_CM";
    static String ProcessDescriptorCMOverrides = "process_descriptor_CM_overrides";
    static String ProcessDescriptorAE = "process_descriptor_AE";
    static String ProcessDescriptorAEOverrides = "process_descriptor_AE_overrides";
    static String ProcessDescriptorCC = "process_descriptor_CC";
    static String ProcessDescriptorCCOverrides = "process_descriptor_CC_overrides";
    static String Timestamp = "timestamp";
    static String Debug = "debug";
  
	CasGenerator casGenerator;
	CasPipeline casPipeline;

    //private boolean timestamp;

    private boolean debug;
	
	public AllInOne(String[] args) throws Exception {
	    for (int i = 0; i < args.length; ++i) {
	        if (i+1 < args.length && !args[i+1].startsWith("--")) {
	            jobRequestProperties.put(args[i].substring(2), args[i+1]);
	            ++i;
	        } else {
	            jobRequestProperties.put(args[i].substring(2), "");
	        }
	  }
      // Properties will have been validated in AllInOneLauncher
      //timestamp = jobRequestProperties.containsKey(Timestamp);
      debug = jobRequestProperties.containsKey(Debug);
	}
	
	private class NoWorkItems extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	private void initialize() throws Exception {
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
	}
	
	private void process() throws Exception {
		String mid = "process";
		int count = 0;
		int total = casGenerator.getTotal();
		mh.frameworkDebug(cid, mid, "total:"+total);
		CAS cas = null;
		while(casGenerator.hasNext()) {
			cas = casGenerator.getCas(cas);
			mh.frameworkDebug(cid, mid, "cas:"+count);
			
			// Emulate a DUCC job by serializing then deserializing into the aggregate's possibly larger typesystem
			String serializedCas = uimaSerializer.serializeCasToXmi(cas);
			CAS cas2 = casPipeline.getEmptyCas();  // Always returns the same CAS
			uimaSerializer.deserializeCasFromXmi(serializedCas, cas2);
			
			casPipeline.process(cas2);
			count++;
		}
		casPipeline.destroy();
	}
	
	private void statistics() {
		if(showStats) {
			casPipeline.dumpStatistics(System.out);
		}
	}
	
	public void go() throws Exception {
		try {
			initialize();
			process();
			statistics();
		}
		catch(NoWorkItems e) {
			String message = "no work items";
			System.err.println("AllInOne.go " + message);
		}
	}
	
	
	public static void main(String[] args) {
		try {
			AllInOne allInOne = new AllInOne(args);
			allInOne.go();
		} catch (Exception e) {
			// Indicate that something went wrong
		    e.printStackTrace();
			System.exit(1);
		}
	}

	class MsgHandler {
	       public void frameworkInfo(String klass, String method, String message) {
	            System.out.println(klass + "." + method + " " + message);
	        }
	    public void frameworkDebug(String klass, String method, String message) {
	        if (debug) System.out.println(klass + "." + method + " " + message);
	    }
	}
}
