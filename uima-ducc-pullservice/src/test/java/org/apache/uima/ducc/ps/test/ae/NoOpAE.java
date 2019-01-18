/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.ps.test.ae;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;


public class NoOpAE extends CasAnnotator_ImplBase
{
    Logger logger;
    static boolean initComplete = false;
    String AE_Identifier = "*^^^^^^^^^ AE ";
    private static AtomicLong processCount = new AtomicLong();
	String errorSequence;

    @Override
    public void initialize(UimaContext uimaContext) throws ResourceInitializationException
    {
    	processCount.set(0);
        super.initialize(uimaContext);
    	errorSequence = System.getProperty("ProcessFail");
    	if ( Objects.isNull(errorSequence)) {
    		errorSequence="";
    	}

        long tid = Thread.currentThread().getId();

        Map<String, String> env = System.getenv();
        RuntimeMXBean rmxb = ManagementFactory.getRuntimeMXBean();
        String pid = rmxb.getName();

        logger = UIMAFramework.getLogger(NoOpAE.class);
        if ( logger == null ) {
            System.out.println("Is this nuts or what, no logger!");
        }

        if ( initComplete ) {
            logger.log(Level.INFO, "Init bypassed in PID:TID " + pid + ":" + tid + ", already completed. ");
            return;
        } else {
        	if ( logger != null )
               logger.log(Level.INFO, "Init procedes in PID:TIDs " + pid + ":" + tid + " Environment:");
            File workingdir = new File(System.getProperty("user.dir"));
            File[] files = workingdir.listFiles();
            if ( logger != null )
               logger.log(Level.INFO, "Working directory " + workingdir.toString() + " has " + files.length + " files.");
       }


        if ( logger != null )
           logger.log(Level.INFO, "^^-------> AE process " + pid + " TID " + tid + " initialization OK");
        return;
    }


    void dolog(Object ... args)
    {
        StringBuffer sb = new StringBuffer();
        for ( Object s : args ) {
            sb.append(s);
            sb.append(" ");
        }
        String s = sb.toString();
        System.out.println("FROM PRINTLN: " + s);
        if ( logger != null )
           logger.log(Level.INFO, "FROM LOGGER:" + s);
    }

    public void destroy()
    {
        System.out.println(AE_Identifier + " Destroy is called (0)");
        dolog("Destroy is called (1) !");
        try {
            Thread.sleep(3000);                         // simulate actual work being done here
        } catch (InterruptedException e) {
        }
        System.out.println(AE_Identifier + " Destroy exits");
    }

    @Override
    public void process(CAS cas) throws AnalysisEngineProcessException
    {
    	long val = processCount.incrementAndGet();
    	//String data = cas.getSofaDataString();
   		String[] errors = errorSequence.split(",");
   		synchronized(NoOpAE.class) {
   			for( String inx : errors) {
   				if ( inx != null && inx.trim().length() > 0 ) {
   	  				long errorSeq = Long.parseLong(inx.trim());
   	   				if ( errorSeq == val ) {
   	   					System.out.println(">>>> Error: errorSeq:"+errorSeq+" processCount:"+val);
   	   		    		throw new AnalysisEngineProcessException(new RuntimeException("Simulated Exception"));
   	   				}

   				}
    		}
   		}
    }


}
