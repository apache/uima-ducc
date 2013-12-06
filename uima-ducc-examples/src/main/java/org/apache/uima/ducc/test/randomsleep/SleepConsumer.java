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

package org.apache.uima.ducc.test.randomsleep;

import java.util.StringTokenizer;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * Minimal CAS consumer for the DD version of the system tests.
 */
public class SleepConsumer extends CasAnnotator_ImplBase 
{
    String CC_Identifier = "*@@@@@@@@@ CC ";

	@Override
	public void initialize(UimaContext uimaContext) throws ResourceInitializationException 
    {
		super.initialize(uimaContext);
        System.out.println(CC_Identifier + "Consumer init called");

	}
	
    public void destroy()
    {
        System.out.println(CC_Identifier + " Destroy is called (0)");
        System.out.println(CC_Identifier + "Destroy exits");
    }

	@Override
	public void process(CAS cas) throws AnalysisEngineProcessException {
		String data = cas.getSofaDataString();

        StringTokenizer tok = new StringTokenizer(data);

        long          elapsed    = Long.parseLong(tok.nextToken());
        int           seqno        = Integer.parseInt(tok.nextToken());
        int           total      = Integer.parseInt(tok.nextToken());
        double        error_rate = Double.parseDouble(tok.nextToken());
        String        logid      = tok.nextToken();

        System.out.println(CC_Identifier + "next returns: " +
                           "Work Item(" + seqno + ") " +
                           "Sleep Time(" + elapsed + ") " +
                           "of total(" + total + ") " +
                           "error_rate(" + error_rate + ") " +
                           "logid(" + logid + ") " +
                           "pass to AE");

	}


}
