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
package org.apache.uima.ducc.common.uima;

import java.util.Random;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;

public class InitializationExceptionPercentageAE extends CasAnnotator_ImplBase {
	
	private Random random = new Random();
	private double percentage = 0.33;
	
	@Override
	public void initialize(UimaContext uimaContext) throws ResourceInitializationException {
		if(random.nextDouble() < percentage) {
			throw new ResourceInitializationException();
		}
	}
	
	@Override
	public void process(CAS cas) throws AnalysisEngineProcessException {
		String data = cas.getSofaDataString();
		if(data == null) {
		}
		try {
			int aeSleepSeconds = 60;
			Thread.sleep(aeSleepSeconds*1000);
		}
		catch(Exception e) {
			
		}
	}

}
