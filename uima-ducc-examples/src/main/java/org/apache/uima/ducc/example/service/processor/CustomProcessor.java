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

package org.apache.uima.ducc.example.service.processor;

import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;

public class CustomProcessor implements IServiceProcessor {

	@Override
	public void initialize() throws ServiceInitializationException {
		
	}

	@Override
	public IProcessResult process(String serializedTask) {
		return new SimpleResult();
	}

	@Override
	public void stop() {
		
	}

	@Override
	public void setScaleout(int scaleout) {
		
	}

	@Override
	public int getScaleout() {
		return 1;
	}

	@Override
	public void setErrorHandlerWindow(int maxErrors, int windowSize) {
		
	}
	public class SimpleResult implements IProcessResult {

		@Override
		public boolean terminateProcess() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getResult() {
			// TODO Auto-generated method stub
			return "";
		}

		@Override
		public String getError() {
			// TODO Auto-generated method stub
			return "";
		}

		@Override
		public Exception getExceptionObject() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
