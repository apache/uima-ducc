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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class NullStateServices implements IStateServices 
{
				
	NullStateServices() {
	}
	
    public boolean init(DuccLogger logger) throws Exception { return true; }
	public ArrayList<Long> getSvcList() 
        throws Exception 
    {
		return new ArrayList<Long>();
	}

	
	public ArrayList<Long> getMetaList() 
        throws Exception 
    {
		return new ArrayList<Long>();
	}
	
	
	public StateServicesDirectory getStateServicesDirectory() 
        throws IOException 
    {
		return new StateServicesDirectory();
	}

    public boolean storeProperties(DuccId serviceId, Properties svc, Properties meta)   throws Exception {return false;}
    public boolean updateProperties(Object dbid, DuccId serviceId, String type, Properties props)    throws Exception {return false;}
    public void    deleteProperties(DuccId serviceId)                                   throws Exception {}
    public void    shutdown()                                                           throws Exception {}
    public void    moveToHHistory()                                                     throws Exception {} 
    public boolean updateJobProperties(Object dbid, DuccId serviceId, Properties props)              throws Exception {return false;}
    public boolean updateMetaProperties(Object dbid, DuccId serviceId, Properties props)             throws Exception {return false;}
    public void    moveToHistory(DuccId serviceId, Properties svc, Properties meta)     throws Exception {}
   
}
