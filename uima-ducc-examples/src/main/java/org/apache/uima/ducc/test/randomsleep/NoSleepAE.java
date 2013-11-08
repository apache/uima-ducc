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

import java.util.Random;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * challngr@us.ibm.com
 * May, 2011
 */

public class NoSleepAE extends CasAnnotator_ImplBase 
{

    Random r;
    long tid = Thread.currentThread().getId();
    Logger logger;
    static boolean initComplete = false;

	@Override
	public void initialize(UimaContext uimaContext) throws ResourceInitializationException {
		super.initialize(uimaContext);
        logger = uimaContext.getLogger();
	}
	
	@Override
	public void process(CAS cas) throws AnalysisEngineProcessException {
		String data = cas.getSofaDataString();
        
        int           qid        = Integer.parseInt(data);
        logger.log(Level.INFO, "Executing task " + qid + ".");
	}

}
