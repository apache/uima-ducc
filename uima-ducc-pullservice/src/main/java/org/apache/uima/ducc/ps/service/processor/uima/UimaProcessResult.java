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
package org.apache.uima.ducc.ps.service.processor.uima;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;

public class UimaProcessResult implements IProcessResult{
	private String metrics;
	private Exception exception;
	private Action action;
	
	UimaProcessResult(String pm) {
		this.metrics = pm;
	}
	UimaProcessResult(Exception exception, Action action) {
		this.exception = exception;
		this.action = action;
		
	}
	@Override
	public boolean terminateProcess() {
		return Action.TERMINATE.equals(action);
	}
	@Override
	public String getResult() {
		return metrics;
	}
	@Override
	public String getError() {
		StringWriter sw = new StringWriter();
		exception.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}

}
