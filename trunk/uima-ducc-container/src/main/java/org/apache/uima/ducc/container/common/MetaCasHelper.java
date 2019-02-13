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
package org.apache.uima.ducc.container.common;

import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;

public class MetaCasHelper {

	private static Logger logger = Logger.getLogger(MetaCasHelper.class, IComponent.Id.JD.name());
	
	private IMetaTask metaCas = null;
	
	public MetaCasHelper(IMetaTask metaCas) {
		setMetaCas(metaCas);
	}
	
	private void setMetaCas(IMetaTask value) {
		metaCas = value;
	}
	
	public int getSystemKey() {
		String location = "getSystemKey";
		int retVal = -1;
		try {
			retVal = Integer.parseInt(metaCas.getSystemKey());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
}
