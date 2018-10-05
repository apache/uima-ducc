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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;

public class DefaultNoTaskAvailableStrategy implements INoTaskAvailableStrategy {
	private int waitTime = 60000;   // default
	private final ReentrantLock lock = new ReentrantLock();
	private final Object monitor = new Object();
	
	public DefaultNoTaskAvailableStrategy(int waitTime) {
		// if wait time not specified use default
		if ( waitTime > 0 ) {
			this.waitTime = waitTime;
		}
	}
	
	public void interrupt() {
		System.out.println("DefaultNoTaskAvailableStrategy.interrupt()"+" Thread:"+Thread.currentThread().getId()+" - Unlocking the lock");
		lock.unlock();
	}
	
	@Override
	public void handleNoTaskSupplied() {
		Condition waitAwhileCondition = lock.newCondition();
		try {
			lock.lock();
			// wait only it wait time > 0. No indefinite wait supported
			if ( waitTime > 0 ) {
				try {
					System.out.println("DefaultNoTaskAvailableStrategy.handleNoTaskSupplied() waiting for:"+waitTime+" Thread:"+Thread.currentThread().getId());
					waitAwhileCondition.await(waitTime, TimeUnit.SECONDS);
				} catch( InterruptedException e) {
					System.out.println("DefaultNoTaskAvailableStrategy.handleNoTaskSupplied() - Waiting interrupted "+" Thread:"+Thread.currentThread().getId());
				}
			}
		} finally {
			lock.unlock();
			System.out.println("DefaultNoTaskAvailableStrategy.handleNoTaskSupplied() - Unlocked Lock "+" Thread:"+Thread.currentThread().getId());

		}
	}
}
