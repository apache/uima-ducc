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
package org.apache.uima.ducc.transport.event.common.history;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;



public class HistoryFactory 
{

	private static IHistoryPersistenceManager instance = null; //new HistoryPersistenceManager();
	
	public static IHistoryPersistenceManager getInstance(String callerClass) 
    {
        if ( instance != null ) return instance;

        String methodName = "getInstance";
        
        // log4j logging annoyance.  We require the caller to give us its base package so
        // we can configure a logger that writes to the right appender
        int ndx = callerClass.lastIndexOf(".");
        String stem = callerClass.substring(0, ndx);

        String clname = System.getProperty("ducc.job.history.impl");

        if ( clname == null ) {
            DuccLogger logger = DuccService.getDuccLogger();
            logger.error(methodName, null, "Job history class is not configured.");
            instance = new NullHistoryManager();
            return instance;
        } 

        ndx = clname.lastIndexOf(".");
        String clfile = clname.substring(ndx+1);
        //
        // We try to construct the persistence object.  If it fails, we return a
        // "null" object conforming to the interface but doing nothing to hopefully
        // reduce NPEs.
        //
        DuccLogger logger = DuccLogger.getLogger(stem + "." + clfile, "DB");  // get the component logger

        //
        // We try to construct the persistence object.  If it fails, we return a
        // "null" object conforming to the interface but doing nothing to hopefully
        // reduce NPEs.
        //

        try {
            @SuppressWarnings("unchecked")
				Class<IStateServices> iss = (Class<IStateServices>) Class.forName(clname);
            instance = (IHistoryPersistenceManager) iss.newInstance();
            instance.setLogger(logger);
        } catch ( Throwable t ) {
            logger.error(methodName, null, "Cannot instantiate service persistence class", clname, ":", t);
            instance = new NullHistoryManager();
        }
        return instance;
	}
	
}
