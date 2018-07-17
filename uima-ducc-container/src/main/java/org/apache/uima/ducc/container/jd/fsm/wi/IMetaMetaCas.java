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
package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.ps.net.iface.IMetaTask;

/**
 * An interface for storing MetaCAS 
 * and CasManager status together
 */
public interface IMetaMetaCas {

	public boolean isExhausted();
	public void setExhausted(boolean value);
	
	public boolean isPremature();
	public void setPremature(boolean value);
	
	public boolean isKillJob();
	public void setKillJob(boolean value);
	
	public IMetaTask getMetaCas();
	public void setMetaCas(IMetaTask value);
}
