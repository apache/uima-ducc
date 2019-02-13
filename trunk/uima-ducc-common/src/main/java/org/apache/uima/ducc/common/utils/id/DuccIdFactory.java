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
package org.apache.uima.ducc.common.utils.id;

import java.util.concurrent.atomic.AtomicLong;

public class DuccIdFactory implements IDuccIdFactory {
	
	private AtomicLong seqno = new AtomicLong(-1);   

	public DuccIdFactory() {	
	}
	
	/*
	 * seed is the first number to give out
	 */
	public DuccIdFactory(long seed) {
		seqno.set(seed-1);
	}
	
    /**
     * During recovery, if you pass in a "friendly", you always want the passed-in friendly, but 
     * you want to ensure that at the end of recovery, the seed is set to the largest of the
     * recovered IDs.
     */
    public DuccId next(long s) {
        seqno.set(Math.max(s, seqno.get()));
        return new DuccId(s);
    }

	public DuccId next() {
		synchronized(this) {
			seqno.incrementAndGet();                
			return new DuccId(seqno.get());
		}
	}
	
	/**
	 * set seqno to candidate if it is bigger than previous, return previous value
	 */
	public long setIfMax(long candidate) {
		long previous = seqno.get();
		synchronized(this) {
			if(candidate > previous) {
				seqno.set(candidate);
			}
		}
		return previous;
	}
}
