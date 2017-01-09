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
package org.apache.uima.ducc.sm;

import java.util.NavigableSet;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class ServiceManagerHelper {

	private static DuccLogger logger = DuccLogger.getLogger(ServiceManagerHelper.class.getName(), SmConstants.COMPONENT_NAME);	
    private static DuccId jobid = null;
	
    /**
     * fetch largest service sequence number from database
     */
    public static long getLargestServiceSeqNo() {
    	String location = "getLargestServiceSeqNo";
    	long seqno = -1;
    	try {
    		IStateServices iss = StateServicesFactory.getInstance(ServiceManagerHelper.class.getName(), SmConstants.COMPONENT_NAME);
    		StateServicesDirectory ssd = iss.getStateServicesDirectory();
    		NavigableSet<Long> keys = ssd.getDescendingKeySet();
    		if(!keys.isEmpty()) {
    			seqno = keys.first();
    		}
    	}
    	catch(Exception e) {
    		logger.error(location, jobid, e);
    	}
    	return seqno;
    }
}
