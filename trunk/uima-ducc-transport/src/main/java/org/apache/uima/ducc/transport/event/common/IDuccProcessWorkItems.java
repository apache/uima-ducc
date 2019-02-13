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

	public boolean isActiveWork();
	public boolean isAssignedWork();
	
	public void setCountDispatch(long value);
	public void setCountDone(long value);
	public void setCountError(long value);
	public void setCountRetry(long value);
	public void setCountPreempt(long value);
	
	public long getCountDispatch();
	public long getCountDone();
	public long getCountError();
	public long getCountRetry();
	public long getCountPreempt();
	
	public void setMillisAvg(long value);
	public void setMillisMax(long value);
	public void setMillisMin(long value);
	
	public long getMillisAvg();
	public long getMillisMax();
	public long getMillisMin();
	
	public long getSecsAvg();
	public long getSecsMax();
	public long getSecsMin();
	
}
