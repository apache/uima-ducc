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
package org.apache.uima.ducc.orchestrator.event;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;

/*
 * Class for OR <--> SM communication to support SM's CLI/API via single point of entry.
 */
public class SmChannel extends DuccEventDispatcher {

	public SmChannel(CamelContext context, String endpoint) {
		super(context, endpoint);
	}

	/*
	 * forward the CLI/API request to SM and get the reply
	 */
	public ServiceReplyEvent exchange(AServiceRequest request) throws Exception {
		DuccEvent duccEvent = dispatchAndWaitForDuccReply(request);
		ServiceReplyEvent reply = (ServiceReplyEvent) duccEvent;
		return reply;
	}
}
