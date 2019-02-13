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

package org.apache.uima.ducc.ps.service.protocol.builtin;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DefaultNoTaskAvailableStrategy implements INoTaskAvailableStrategy {
	private long waitTime;   
	Logger logger = UIMAFramework.getLogger(DefaultNoTaskAvailableStrategy.class);
	public DefaultNoTaskAvailableStrategy(long waitTimeInMillis) {
		this.waitTime = waitTimeInMillis;
		logger.log(Level.INFO, ">>>>>>>> Service Wait Time For Task:"+waitTimeInMillis+" ms");
	}
	/**
	 * This methods is called when a service is stopping. There is no
	 * need to wait. We want to stop as soon as possible so we just
	 * interrupt the thread which might be blocking in sleep()
	 */
	@Override
	public void interrupt() {
		Thread.currentThread().interrupt();
	}
	
	@Override
	public void handleNoTaskSupplied() {
		try {
			Thread.sleep(waitTime);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
		}

	}
	@Override
	public long getWaitTimeInMillis() {
		return waitTime;
	}
	
}
