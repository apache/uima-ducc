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
package org.apache.uima.ducc.container.common.fault.injector;

import java.util.Random;

import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;

public class FaultInjector {

	public static boolean quitAfterGet = false;
	public static boolean quitAfterAck = false;
	
	private static int seed = 1;
	private static int pctFail = 25;
	private static Random random = new Random(seed);
	
	public static long getTimeToFailure(IMetaCasTransaction trans) {
		long time = 0;
		Type type = trans.getType();
		switch(type) {
		case Get:
			if(quitAfterGet) {
				if(random.nextInt() < pctFail) {
					long slack = 10;
					long delay = 60;
					time = (delay+slack)*1000;
				}
			}
			break;
		case Ack:
			if(quitAfterAck) {
				if(random.nextInt() < pctFail) {
					long slack = 10;
					long delay = 60;
					time = (delay+slack)*1000;
				}
			}
			break;
		case End:
			break;
		}
		return time;
	}

}
