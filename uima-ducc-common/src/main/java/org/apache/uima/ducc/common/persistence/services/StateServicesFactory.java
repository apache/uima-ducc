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
package org.apache.uima.ducc.common.persistence.services;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;



public class StateServicesFactory 
{
    private static IStateServices instance = null;

	private static IStateServices getInstanceInternal(String callerClass, String component) 
    {
        String methodName = "getInstance";
		// log4j logging annoyance.  We require the caller to give us its base package so
        // we can configure a logger that writes to the right appender
       // log4j logging annoyance.  We require the caller to give us its base package so
        // we can configure a logger that writes to the right appender
        int ndx = callerClass.lastIndexOf(".");
        String stem = callerClass.substring(0, ndx);

        String clname = System.getProperty("ducc.service.persistence.impl");
        if ( clname == null ) {
        	DuccLogger logger = DuccService.getDuccLogger();
        	logger.warn(methodName, null, "Service persistence manager is not configured.  Returning null instance.");
            return new NullStateServices();
        }
        ndx = clname.lastIndexOf(".");
        String clfile = clname.substring(ndx+1);
        //
        // We try to construct the persistence object.  If it fails, we return a
        // "null" object conforming to the interface but doing nothing to hopefully
        // reduce NPEs.
        //
        DuccLogger logger = DuccLogger.getLogger(stem + "." + clfile, "DB");  // get the component logger

        IStateServices ret = null;
        try {
            @SuppressWarnings("unchecked")
				Class<IStateServices> iss = (Class<IStateServices>) Class.forName(clname);
            ret = (IStateServices) iss.newInstance();
            ret.init(logger);
        } catch ( Throwable t ) {
            logger.error(methodName, null, "Cannot instantiate service persistence class", clname, ":", t);
            ret = new NullStateServices();
        }

        return ret;
	}

	public static IStateServices getInstance(String callerClass, String component) 
    {
        if ( instance == null ) {
            instance = getInstanceInternal(callerClass, component);
        }

        return instance;
	}
	
}
