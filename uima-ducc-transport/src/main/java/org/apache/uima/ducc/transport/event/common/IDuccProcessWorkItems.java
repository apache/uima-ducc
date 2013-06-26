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
package org.apache.uima.ducc.transport.event.common;

import java.io.Serializable;

public interface IDuccProcessWorkItems extends Serializable {
	
	public long getCountDispatch();
	public long getCountDone();
	public long getCountError();
	public long getCountRetry();
	public long getCountLost();
	public long getCountPreempt();
	
	public long getSecsAvg();
	public long getSecsMax();
	public long getSecsMin();
	
	public void dispatch();
	public void done(long millis);
	public void error();
	public void retry();
	public void lost();
	public void preempt();
}
