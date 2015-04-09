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

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.classloader.ProxyException;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class CasManager {

	private static Logger logger = Logger.getLogger(CasManager.class, IComponent.Id.JD.name());
	
	private ProxyJobDriverCollectionReader pjdcr = null;
	
	private LinkedBlockingQueue<IMetaCas> cacheQueue = new LinkedBlockingQueue<IMetaCas>();
	
	private CasManagerStats casManagerStats = new CasManagerStats();

	public CasManager() throws JobDriverException {
		initialize();
	}
	
	private void initialize() throws JobDriverException {
		String location = "initialize";
		try {
			pjdcr = new ProxyJobDriverCollectionReader();
			casManagerStats.setCrTotal(pjdcr.getTotal());
		}
		catch(ProxyException e) {
			logger.error(location, ILogger.null_id, e);
			throw new JobDriverException();
		}
	}
	
	public IMetaCas getMetaCas() throws JobDriverException {
		String location = "getMetaCas";
		try {
			IMetaCas retVal = dequeueMetaCas();
			if(retVal == null) {
				retVal = pjdcr.getMetaCas();
				if(retVal != null) {
					casManagerStats.incCrGets();
				}
			}
			return retVal;
		}
		catch(ProxyException e) {
			logger.error(location, ILogger.null_id, e);
			throw new JobDriverException();
		}
	}

	private IMetaCas dequeueMetaCas() throws JobDriverException {
		String location = "dequeueMetaCas";
		IMetaCas metaCas = cacheQueue.poll();
		if(metaCas != null) {
			casManagerStats.incRetryQueueGets();
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
			mb.append(Standardize.Label.puts.get()+casManagerStats.getRetryQueuePuts());
			mb.append(Standardize.Label.gets.get()+casManagerStats.getRetryQueueGets());
			logger.debug(location, ILogger.null_id, mb);
		}
		return metaCas;
	}
	
	public void putMetaCas(IMetaCas metaCas, RetryReason retryReason) {
		queueMetaCas(metaCas, retryReason);
	}
	
	private void queueMetaCas(IMetaCas metaCas, RetryReason retryReason) {
		String location = "queueMetaCas";
		cacheQueue.add(metaCas);
		casManagerStats.incRetryQueuePuts();
		casManagerStats.incRetryReasons(retryReason);
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.puts.get()+casManagerStats.getRetryQueuePuts());
		mb.append(Standardize.Label.gets.get()+casManagerStats.getRetryQueueGets());
		mb.append(Standardize.Label.reason.get()+retryReason.name());
		logger.debug(location, ILogger.null_id, mb);
	}
	
	public CasManagerStats getCasManagerStats() {
		return casManagerStats;
	}
}
