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
package org.apache.uima.ducc.ps.service.transport.target;

import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;

public class TargetURIFactory {
	private TargetURIFactory() {}
	
	public static ITargetURI newTarget(String targetAsString) throws ServiceException {
		if ( targetAsString.toLowerCase().startsWith("http:")) {
			return new HttpTargetURI(targetAsString);
		} else if ( targetAsString.toLowerCase().startsWith("tcp:")) {
			return new SocketTargetURI(targetAsString);
		} else {
			throw new ServiceException("Registry provider unsupported URL protocol - Expected either tcp or http - Instead got "+targetAsString);
		}
	}
}
