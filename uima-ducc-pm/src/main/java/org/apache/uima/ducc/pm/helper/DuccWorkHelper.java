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
package org.apache.uima.ducc.pm.helper;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.pm.ProcessManager;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.dispatcher.IDuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DuccWorkReplyEvent;
import org.apache.uima.ducc.transport.event.DuccWorkRequestEvent;
import org.apache.uima.ducc.transport.event.common.IDuccWork;

public class DuccWorkHelper {

	public static DuccLogger logger = DuccLogger.getLogger(DuccWorkHelper.class, ProcessManager.DuccComponent);
	
	private IDuccEventDispatcher dispatcher = null;
	private DuccId jobid = null;
	private String orchestrator = "orchestrator";
	
	public DuccWorkHelper() {
		init();
	}
	
	private void init() {
		String location = "init";
		try {
			String targetUrl = getTargetUrl();
			dispatcher = new DuccEventHttpDispatcher(targetUrl);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private String getServer() {
		return orchestrator;
	}
	
	private String getTargetUrl() {
		String location = "getTargetUrl";
		String targetUrl = null;
		String server = getServer();
		String host = DuccPropertiesResolver.get("ducc." + server + ".http.node");
	    String port = DuccPropertiesResolver.get("ducc." + server + ".http.port");
        if ( host == null || port == null ) {
        	String message = "ducc." + server + ".http.node and/or .port not set in ducc.properties";
            throw new IllegalStateException(message);
        }
        targetUrl = "http://" + host + ":" + port + "/" + server.substring(0, 2);
        logger.info(location, jobid, targetUrl);
		return targetUrl;
	}
	
	public IDuccWork fetch(DuccId duccId) {
		String location = "fetch";
		IDuccWork dw = null;
		if(duccId != null) {
			DuccWorkRequestEvent dwRequestEvent = new DuccWorkRequestEvent(duccId);
			DuccWorkReplyEvent dwReplyEvent = null;
			try {
				dwReplyEvent = (DuccWorkReplyEvent) dispatcher.dispatchAndWaitForDuccReply(dwRequestEvent);
				if(dwReplyEvent != null) {
					dw = dwReplyEvent.getDw();
					if(dw == null) {
						logger.debug(location, duccId, "value is null");
					}
					else {
						logger.debug(location, duccId, "state is "+dw.getStateObject());
					}
				}
				else {
					logger.debug(location, duccId, "reply is null");
				}
			} 
			catch (Exception e) {
				logger.error(location, duccId, "Error while communicating with the OR:\n"+e);
			}
		}
		else {
			logger.debug(location, duccId, "key is null");
		}
		return dw;
	}
	
}
