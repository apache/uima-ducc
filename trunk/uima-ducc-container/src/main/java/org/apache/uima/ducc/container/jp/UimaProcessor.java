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

package org.apache.uima.ducc.container.jp;

import java.lang.reflect.Method;

import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;

public class UimaProcessor implements IUimaProcessor {

	Method processMethod=null;
	Method stopMethod=null;
	Method lastErrorMethod=null;
	
	Object uimaContainerInstance = null;
	int scaleout;
	volatile boolean running = false;
	public UimaProcessor(Object uimaContainerInstance, Method processMethod, Method stopMethod, Method lastErrorMethod, int scaleout) {
		this.processMethod = processMethod;
		this.stopMethod = stopMethod;
		this.lastErrorMethod = lastErrorMethod;
		this.uimaContainerInstance = uimaContainerInstance;
		this.scaleout = scaleout;
		this.running = true;
	}
	
	public void stop() throws Exception {
		running = false;
		stopMethod.invoke(uimaContainerInstance);
	}

	
	public Object process(Object xmi) throws Exception {
		return processMethod.invoke(uimaContainerInstance, xmi);
/*
		if ( running ) {
			return processMethod.invoke(uimaContainerInstance, xmi);
		} else {
			throw new IllegalStateException("UimaProcessor Not in Running State - The Service is in Stopping State");
		}
		*/
	}

	public byte[] getLastSerializedError() throws Exception {
		return (byte[])lastErrorMethod.invoke(uimaContainerInstance);
	}
	public int getScaleout() throws Exception {
		return scaleout;
	}

}
