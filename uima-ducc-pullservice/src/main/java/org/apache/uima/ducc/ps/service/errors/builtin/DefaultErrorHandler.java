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

package org.apache.uima.ducc.ps.service.errors.builtin;

import org.apache.uima.ducc.ps.service.IServiceComponent;
import org.apache.uima.ducc.ps.service.Lifecycle;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.metrics.IWindowStats;


public class DefaultErrorHandler implements IServiceErrorHandler {
	private int frameWorkErrorLimit=-1; // no limit
	private Action actionOnProcessError;
	private int windowSize = 0;
	private Action actionOnExceedsWindowSize;
	private Lifecycle lifecycleMonitor;
	private long errorCount=0;
	
	public DefaultErrorHandler(Action action) {
		
		this.actionOnProcessError = action;
	}
	public DefaultErrorHandler() {
		this.actionOnProcessError = Action.TERMINATE;
	}
	
	public DefaultErrorHandler withMaxFrameworkErrors(int maxFrameworkError) {
		this.frameWorkErrorLimit = maxFrameworkError;
		return this;
	}
	public DefaultErrorHandler withProcessErrorWindow(int errorWindow, Action errorAction ) {
		this.actionOnExceedsWindowSize = errorAction;
		this.windowSize = errorWindow;
		return this;
	}
	private boolean exceedsProcessWindow() {
		return (errorCount % windowSize == 0);
	}
	@Override
	public Action handleProcessError(Exception e, IServiceComponent source, IWindowStats stats) {
		
		return Action.TERMINATE;
	}
	@Override
	public Action handle(Exception e, IServiceComponent source) {
		errorCount++;
		if ( exceedsProcessWindow() ) {
			Thread t = new Thread( new Runnable() {
				public void run() {
					lifecycleMonitor.stop();
				}
			});
			t.start();
			
		}
		
		
		
		return Action.TERMINATE;	}
	
}
