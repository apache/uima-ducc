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
package org.apache.uima.ducc.container.jd.cas;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.DriverState;

public class CasManager {

	private static Logger logger = Logger.getLogger(CasManager.class, IComponent.Id.JD.name());
	
	private ProxyJobDriverCollectionReader pjdcr = null;
	
	private LinkedBlockingQueue<IMetaCas> cacheQueue = new LinkedBlockingQueue<IMetaCas>();
	
	private CasManagerStats casManagerStats = new CasManagerStats();

	public CasManager() throws JobDriverException {
		initialize();
	}
	
	public void initialize() throws JobDriverException {
		String location = "initialize";
		try {
			pjdcr = new ProxyJobDriverCollectionReader();
			casManagerStats.setCrTotal(pjdcr.getTotal());
			JobDriver.getInstance().advanceDriverState(DriverState.Active);
		}
		catch(JobDriverException e) {
			logger.error(location, ILogger.null_id, e);
			throw e;
		}
	}
	
	public IMetaCas getMetaCas() throws JobDriverException {
		IMetaCas retVal = cacheQueue.poll();
		if(retVal != null) {
			casManagerStats.incRetryQueueGets();
		}
		else {
			retVal = pjdcr.getMetaCas();
			if(retVal != null) {
				casManagerStats.incCrGets();
			}
		}
		return retVal;
	}
	
	public void putMetaCas(IMetaCas metaCas, RetryReason retryReason) {
		cacheQueue.add(metaCas);
		casManagerStats.incRetryQueuePuts();
		casManagerStats.incRetryReasons(retryReason);
	}
	
	public CasManagerStats getCasManagerStats() {
		return casManagerStats;
	}
}
