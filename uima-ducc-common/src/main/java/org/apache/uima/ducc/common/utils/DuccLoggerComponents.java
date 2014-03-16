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
package org.apache.uima.ducc.common.utils;

public class DuccLoggerComponents implements IDuccLoggerComponents {
	
	static public DuccLogger makeLogger(String claz, String componentId) {
        return DuccLogger.getLogger(claz, componentId);
	}
	
	static public DuccLogger getJdOut(String claz)
    {
        return makeLogger(claz, abbrv_jobDriver);
    }
	
	static public DuccLogger getJdErr(String claz)
    {
        return makeLogger("org.apache.uima.ducc.user.err", abbrv_jobDriver);
    }
	
	static public DuccLogger getDbLogger(String claz)
    {
        return makeLogger(claz, abbrv_db);
    }
	
	static public DuccLogger getOrLogger(String claz)
    {
        return makeLogger(claz, abbrv_orchestrator);
    }
	
	static public DuccLogger getTrLogger(String claz)
    {
        return makeLogger(claz, abbrv_transport);
    }
	
	static public DuccLogger getSmLogger(String claz)
    {
        return makeLogger(claz, abbrv_servicesManager);
    }
	
	static public DuccLogger getWsLogger(String claz)
    {
        return makeLogger(claz, abbrv_webServer);
    }

}
