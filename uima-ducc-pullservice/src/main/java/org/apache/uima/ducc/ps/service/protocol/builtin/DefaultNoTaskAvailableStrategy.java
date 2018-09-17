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

import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;

public class DefaultNoTaskAvailableStrategy implements INoTaskAvailableStrategy {
	private int waitTime = 60000;   // default
	private static final Object monitor = new Object();
	
	public DefaultNoTaskAvailableStrategy(int waitTime) {
		// if wait time not specified use default
		if ( waitTime > 0 ) {
			this.waitTime = waitTime;
		}
	}
	@Override
	public void handleNoTaskSupplied() {
		synchronized(monitor) {
			// wait only it wait time > 0. No indefinite wait supported
			if ( waitTime > 0 ) {
				try {
					System.out.println("DefaultNoTaskAvailableStrategy.handleNoTaskSupplied() waiting for:"+waitTime);
					monitor.wait(waitTime);
				} catch( InterruptedException e) {
				}
			}
		}
	}
}
