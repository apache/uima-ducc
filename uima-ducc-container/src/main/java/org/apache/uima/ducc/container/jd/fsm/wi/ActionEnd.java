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
package org.apache.uima.ducc.container.jd.fsm.wi;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateKeeper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.MetaCasHelper;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.user.error.classload.ProxyUserErrorException;
import org.apache.uima.ducc.container.jd.user.error.classload.ProxyUserErrorStringify;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceKeeper;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;

public class ActionEnd extends ActionEndAbstract implements IAction {

	private static Logger logger = Logger.getLogger(ActionEnd.class, IComponent.Id.JD.name());
	
	private ProxyUserErrorStringify proxy = null;
	
	public ActionEnd() {
		super(logger);
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		try {
			proxy = new ProxyUserErrorStringify();
		} 
		catch (ProxyUserErrorException e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public String getName() {
		return ActionEnd.class.getName();
	}
	
	private void successWorkItem(IActionData actionData, CasManager cm, IWorkItem wi) {
		String location = "successWorkItem";
		cm.getCasManagerStats().incEndSuccess();
		wi.setTodEnd();
		updateStatistics(actionData, wi);
		updatePerformanceMetrics(actionData, wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.debug(location, ILogger.null_id, mb.toString());
		checkEnded(actionData, cm);
	}
	
	private void updateStatistics(IActionData actionData, IWorkItem wi) {
		String location = "updateStatistics";
		IWorkItemStatistics wis = JobDriver.getInstance().getWorkItemStatistics();
		wis.ended(wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.avg.get()+wis.getMillisAvg());
		mb.append(Standardize.Label.max.get()+wis.getMillisMax());
		mb.append(Standardize.Label.min.get()+wis.getMillisMin());
		logger.debug(location, ILogger.null_id, mb.toString());
	}

	private String keyName = "name";
	private String keyUniqueName = "uniqueName";
	private String keyAnalysisTime = "analysisTime";
	
	private void updatePerformanceMetrics(IActionData actionData, IWorkItem wi) {
		String location = "updatePerformanceMetrics";
		if(wi != null) {
			IMetaCas metaCas = wi.getMetaCas();
			if(metaCas != null) {
				IPerformanceMetrics performanceMetrics = metaCas.getPerformanceMetrics();
				if(performanceMetrics != null) {
					List<Properties> list = performanceMetrics.get();
					if(list != null) {
						int size = 0;
						if(list !=  null) {
							size = list.size();
							JobDriver jd = JobDriver.getInstance();
							IWorkItemPerformanceKeeper wipk = jd.getWorkItemPerformanceKeeper();
							wipk.count();
							for(Properties properties : list) {
								String name = properties.getProperty(keyName);
								String uniqueName = properties.getProperty(keyUniqueName);
								String analysisTime = properties.getProperty(keyAnalysisTime);
								long time = 0;
								try {
									time = Long.parseLong(analysisTime);
								}
								catch(Exception e) {
									logger.error(location, ILogger.null_id, e);
								}
								wipk.dataAdd(name, uniqueName, time);
								for(Entry<Object, Object> entry : properties.entrySet()) {
									String key = (String) entry.getKey();
									String value = (String) entry.getValue();
									MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
									mb.append(Standardize.Label.key.get()+key);
									mb.append(Standardize.Label.value.get()+value);
									logger.debug(location, ILogger.null_id, mb.toString());
								}
							}
						}
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append(Standardize.Label.size.get()+size);
						logger.debug(location, ILogger.null_id, mb.toString());
					}
				}
			}
		}
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			if(actionData != null) {
				IWorkItem wi = actionData.getWorkItem();
				IMetaCasTransaction trans = actionData.getMetaCasTransaction();
				IRemoteWorkerProcess rwp = new RemoteWorkerProcess(trans);
				IMetaCas metaCas = wi.getMetaCas();
				JobDriver jd = JobDriver.getInstance();
				JobDriverHelper jdh = JobDriverHelper.getInstance();
				CasManager cm = jd.getCasManager();
				//
				IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
				MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
				IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
				//
				if(metaCas != null) {
					WiTracker.getInstance().unassign(wi);
					//
					TimeoutManager toMgr = TimeoutManager.getInstance();
					toMgr.receivedAck(actionData);
					toMgr.receivedEnd(actionData);
					//
					int seqNo = metaCasHelper.getSystemKey();
					Object exception = metaCas.getUserSpaceException();
					if(exception != null) {
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append("exception");
						logger.info(location, ILogger.null_id, mb.toString());
						Object userException = metaCas.getUserSpaceException();
						String printableException = null;
						try {
							printableException = proxy.convert(userException);
						}
						catch(Exception e) {
							logger.error(location, ILogger.null_id, e);
						}
						handleException(actionData, userException, printableException);
						displayProcessStatistics(logger, actionData, wi, pStats);
					}
					else {
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append("ended");
						logger.info(location, ILogger.null_id, mb.toString());
						wisk.ended(seqNo);
						successWorkItem(actionData, cm, wi);
						pStats.done(wi);
						displayProcessStatistics(logger, actionData, wi, pStats);
					}
					wi.reset();
				}
				else {
					MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
					mb.append("No CAS found for processing");
					logger.info(location, ILogger.null_id, mb.toString());
				}
			}
			else {
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				mb.append("No action data found for processing");
				logger.warn(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

}
