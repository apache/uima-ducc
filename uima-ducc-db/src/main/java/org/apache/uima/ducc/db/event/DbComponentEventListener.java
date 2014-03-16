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
package org.apache.uima.ducc.db.event;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.db.IDbComponent;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;

public class DbComponentEventListener implements DuccEventDelegateListener {
	
	private DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DbComponentEventListener.class.getName());
	private DuccId jobid = null;
	
	private IDbComponent dbComponent;
	
	public DbComponentEventListener(IDbComponent value) {
		String location = "DbComponentEventListener";
		dbComponent = value;
		duccLogger.debug(location, jobid, dbComponent.getClass().getName());
	}
	
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) {
	}
	
	public void setEndpoint( String endpoint ) {
	}

}
