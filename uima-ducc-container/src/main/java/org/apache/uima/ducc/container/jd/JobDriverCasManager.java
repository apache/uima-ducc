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
package org.apache.uima.ducc.container.jd;

import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.jd.classload.JobDriverCollectionReader;
import org.apache.uima.ducc.container.net.impl.MetaCas;

public class JobDriverCasManager {

	private IContainerLogger logger = ContainerLogger.getLogger(JobDriverCasManager.class, IContainerLogger.Component.JD.name());
	
	private JobDriverCollectionReader jdcr = null;
	
	private LinkedBlockingQueue<MetaCas> cacheQueue = new LinkedBlockingQueue<MetaCas>();
	
	private AtomicInteger countGet = new AtomicInteger(0);
	private AtomicInteger countPut = new AtomicInteger(0);
	private AtomicInteger countGetCr = new AtomicInteger(0);
	
	public JobDriverCasManager(String[] classpath, String crXml, String crCfg) throws JobDriverException {
		initialize(classpath, crXml, crCfg);
	}
	
	public void initialize(String[] classpath, String crXml, String crCfg) throws JobDriverException {
		String location = "initialize";
		try {
			URL[] classLoaderUrls = new URL[classpath.length];
			int i = 0;
			for(String item : classpath) {
				classLoaderUrls[i] = this.getClass().getResource(item);
				i++;
			}
			jdcr = new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
		}
		catch(JobDriverException e) {
			logger.error(location, IEntityId.null_id, e);
			throw e;
		}
	}
	
	public MetaCas getMetaCas() throws JobDriverException {
		MetaCas retVal = cacheQueue.poll();
		if(retVal != null) {
			countGet.incrementAndGet();
		}
		else {
			retVal = jdcr.getMetaCas();
			if(retVal != null) {
				countGetCr.incrementAndGet();
				countGet.incrementAndGet();
			}
		}
		return retVal;
	}
	
	public void putMetaCas(MetaCas metaCas) {
		cacheQueue.add(metaCas);
		countPut.incrementAndGet();
	}
	
	public int getTotal() throws JobDriverException {
		return jdcr.getTotal();
	}
	
	public int getGets() {
		return countGet.intValue();
	}
	
	public int getPuts() {
		return countPut.intValue();
	}
	
	public int getGetsCr() {
		return countGetCr.intValue();
	}
}
