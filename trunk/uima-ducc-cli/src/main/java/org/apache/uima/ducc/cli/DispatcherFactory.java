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
package org.apache.uima.ducc.cli;

import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcherCl;
import org.apache.uima.ducc.transport.dispatcher.IDuccEventDispatcher;

public class DispatcherFactory {

	public static IDuccEventDispatcher create(DuccProperties cli_props, String servlet) throws Exception {
		IDuccEventDispatcher retVal = null;
		String targetUrl = DuccUiUtilities.dispatchUrl(servlet);
		retVal = new DuccEventHttpDispatcherCl(targetUrl, -1);
		return retVal;
	}
}
