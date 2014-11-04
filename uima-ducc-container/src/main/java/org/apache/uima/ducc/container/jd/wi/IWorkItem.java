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
package org.apache.uima.ducc.container.jd.wi;

import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public interface IWorkItem {
	
	public void setMetaCas(IMetaCas value);
	public IMetaCas getMetaCas();
	
	public void setFsm(IFsm value);
	public IFsm getFsm();
	
	public void resetTods();
	
	public void setTodGet();
	public void resetTodGet();
	public long getTodGet();
	
	public void setTodAck();
	public void resetTodAck();
	public long getTodAck();
	
	public void setTodEnd();
	public void resetTodEnd();
	public long getTodEnd();
	
	public long getMillisOperating();
}
