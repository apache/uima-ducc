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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;


public class NullHistoryManager 
    implements IHistoryPersistenceManager 
{
	NullHistoryManager() 
    {
	}
	
	public void setLogger(DuccLogger logger) {}
	
	public void saveJob(IDuccWorkJob duccWorkJob) 
        throws Exception 
    {
	}
	
	public IDuccWorkJob restoreJob(long duccid)
        throws Exception
    {
        return null;
	}
	
	public List<IDuccWorkJob> restoreJobs(long max) 
        throws Exception
    {
		return  new ArrayList<IDuccWorkJob>();
	}


	public void saveReservation(IDuccWorkReservation reservation) 
        throws Exception
    {
    }

	public IDuccWorkReservation    restoreReservation(long friendly_id) 
        throws Exception
    {
        return null;
    }

	public List<IDuccWorkReservation> restoreReservations(long max)   
        throws Exception
    {
        return new ArrayList<IDuccWorkReservation>();
    }


	public void saveService(IDuccWorkService service)
        throws Exception
    {
    }


	public void serviceSave(IDuccWorkService service)
	        throws Exception
	    {
	    }
	

	public IDuccWorkService restoreService(long duccid)
        throws Exception
    {
        return null;
	}
	
	public List<IDuccWorkService> restoreServices(long max) 
        throws Exception
    {
		return  new ArrayList<IDuccWorkService>();
	}

	
	public IDuccWorkService serviceRestore(String fileName) 
    {
        return null;
	}
	
	public ArrayList<String> serviceList() 
    {
		return new ArrayList<String>();
	}

	
	public ArrayList<IDuccWorkService> serviceRestore() 
        throws IOException,
               ClassNotFoundException 
    {
		return new ArrayList<IDuccWorkService>();
	}

	
	public IDuccWorkService serviceRestore(DuccId duccId) 
    {
        return null;
	}

    public boolean checkpoint(DuccWorkMap work, Map<DuccId, DuccId> processToJob)   throws Exception { return false; }
    public boolean restore(DuccWorkMap work, Map<DuccId, DuccId> processToJob)      throws Exception { return false; }
	
}
