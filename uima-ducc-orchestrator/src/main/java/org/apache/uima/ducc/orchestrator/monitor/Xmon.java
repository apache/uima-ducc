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
package org.apache.uima.ducc.orchestrator.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorAbbreviatedStateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;
import org.apache.uima.ducc.transport.event.jd.IDriverState.DriverState;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;


public class Xmon implements Processor {

	private static final DuccLogger logger = DuccLogger.getLogger(Xmon.class, "XM");
	private static final DuccId duccId = null;
	
	public static enum LifeStatus { Start, Ended, Error, Timer };
	public static enum ExchangeType { Send, Receive, Reply };
	
	private static enum LogType { Debug, Info, Warn, Error };
	
	private static LogType defaultLogType = LogType.Info;
	
	private static AtomicLong sequence = new AtomicLong(-1);
	
	private LifeStatus lifeStatus;
	private ExchangeType exchangeType;
	@SuppressWarnings("rawtypes")
	private Class xclass;
	
	private static long ReasonableElapsedMillis = 10 * 1000;
	
	private static String keySequence  = "ducc.exchange.monitor.sequencer";
	private static String keyStartMillis  = "ducc.exchange.monitor.start.millis";
	
	private void putSequence(Exchange exchange) {
		String key = keySequence;
		String value = String.format("%06d", sequence.incrementAndGet());
		exchange.setProperty(key, value);
	}
	
	private String getSequence(Exchange exchange) {
		String key = keySequence;
		String value = (String)exchange.getProperty(key);
		return value;
	}

	private void putStartMillis(Exchange exchange) {
		String key = keyStartMillis;
		Long value = new Long(System.currentTimeMillis());
		exchange.setProperty(key, value);
	}
	
	private Long getStartMillis(Exchange exchange) {
		String key = keyStartMillis;
		Long value = (Long)exchange.getProperty(key);
		if(value == null) {
			value = new Long(System.currentTimeMillis());
		}
		return value;
	}

	public Xmon(LifeStatus lifeStatus, ExchangeType exchangeType) {
		setLifeStatus(lifeStatus);
		setExchangeType(exchangeType);
	}
	
	public Xmon(LifeStatus lifeStatus, ExchangeType exchangeType, @SuppressWarnings("rawtypes") Class xclass) {
		setLifeStatus(lifeStatus);
		setExchangeType(exchangeType);
		setClass(xclass);
	}
	

	private LifeStatus getLifeStatus() {
		return this.lifeStatus;
	}
	
	private void setLifeStatus(LifeStatus lifeStatus) {
		this.lifeStatus = lifeStatus;
	}
	
	private void setExchangeType(ExchangeType exchangeType) {
		this.exchangeType = exchangeType;
	}
	
	private void setClass(@SuppressWarnings("rawtypes") Class xclass) {
		this.xclass = xclass;
	}
	
	private void timex(Exchange exchange, String exchId, String event) {
		switch(lifeStatus) {
		case Start:
			break;
		default:
			long t0 = getStartMillis(exchange);
			long t1 = System.currentTimeMillis();
			long elapsed = t1-t0;
			if(elapsed > ReasonableElapsedMillis) {
				String details = "elapsed:"+elapsed;
				LifeStatus save = getLifeStatus();
				setLifeStatus(LifeStatus.Timer);
				log(exchange, LogType.Warn, exchId, event, details);
				setLifeStatus(save);
			}
			break;
		}
	}
	
	
	public void process(Exchange exchange) throws Exception {
		String location = "process";
		try {
			switch(lifeStatus) {
			case Start:
				putSequence(exchange);
				putStartMillis(exchange);
				break;
			}
			switch(exchangeType) {
			case Receive:
				processReceive(exchange);
				break;
			case Send:
				processSend(exchange);
				break;
			case Reply:
				processReply(exchange);
				break;
			}
		}
		catch(Exception e) {
			logger.error(location, duccId, e);
		}
	};
	
	private void processReceive(Exchange exchange) throws Exception {
		Object body = exchange.getIn().getBody();
		if(body instanceof SubmitJobDuccEvent) {
			processReceive(exchange, (SubmitJobDuccEvent)body);
		}
		else if(body instanceof CancelJobDuccEvent) {
			processReceive(exchange, (CancelJobDuccEvent)body);
		}
		else if(body instanceof SubmitReservationDuccEvent) {
			processReceive(exchange, (SubmitReservationDuccEvent)body);
		}
		else if(body instanceof CancelReservationDuccEvent) {
			processReceive(exchange, (CancelReservationDuccEvent)body);
		}
		else if(body instanceof SubmitServiceDuccEvent) {
			processReceive(exchange, (SubmitServiceDuccEvent)body);
		}
		else if(body instanceof CancelServiceDuccEvent) {
			processReceive(exchange, (CancelServiceDuccEvent)body);
		}
		else if(body instanceof SmStateDuccEvent) {
			processReceive(exchange, (SmStateDuccEvent)body);
		}
		else if(body instanceof RmStateDuccEvent) {
			processReceive(exchange, (RmStateDuccEvent)body);
		}
		else if(body instanceof JdStateDuccEvent) {
			processReceive(exchange, (JdStateDuccEvent)body);
		}
		else if(body instanceof NodeInventoryUpdateDuccEvent) {
			processReceive(exchange, (NodeInventoryUpdateDuccEvent)body);
		}
		else {
			processUnexpected(exchange, (DuccEvent)body);
		}
	}
	
	private void processReceive(Exchange exchange, SubmitJobDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, CancelJobDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, SubmitReservationDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, CancelReservationDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, SubmitServiceDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, CancelServiceDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, SmStateDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		int count = 0;
		ServiceMap map = duccEvent.getServiceMap();
		if(map != null) {
			count = map.size();
		}
		String details = "count:"+count;
		log(exchange, defaultLogType, exchId, event, details);
		timex(exchange, exchId, event);
	}
	
	private void processReceive(Exchange exchange, RmStateDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		Map<DuccId, IRmJobState> map = duccEvent.getJobState();
		if(map != null) {
			Iterator<Entry<DuccId, IRmJobState>> iterator = map.entrySet().iterator();
			int countJ = 0;
			int countR = 0;
			int countS = 0;
			int countO = 0;
			while(iterator.hasNext()) {
				Entry<DuccId, IRmJobState> entry = iterator.next();
				IRmJobState resource = entry.getValue();
				switch(resource.getDuccType()) {
				case Job:
					countJ++;
					break;
				case Reservation:
					countR++;
					break;
				case Service:
					countS++;
					break;
				default:
					countO++;
					break;
				}
			}
			String details = "jobs:"+countJ+" "+"reservations:"+countR+" "+"services:"+countS+" "+"other:"+countO;
			log(exchange, defaultLogType, exchId, event, details);
			timex(exchange, exchId, event);
		}
		else {
			log(exchange, LogType.Warn, exchId, event, "null map?");
			timex(exchange, exchId, event);
		}
	}
	
	private void processReceive(Exchange exchange, JdStateDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		DriverStatusReport dsr = duccEvent.getState();
		if(dsr == null) {
			String details = "dsr:"+dsr;
			log(exchange, defaultLogType, exchId, event, details);
			timex(exchange, exchId, event);
		}
		else {
			String id = dsr.getId();
			DriverState driverState = dsr.getDriverState();
			if(driverState != null) {
				String state = dsr.getDriverState().toString();
				int threadCount = dsr.getThreadCount();
				int wiFetch = dsr.getWorkItemsFetched();
				int wiCompleted = dsr.getWorkItemsProcessingCompleted();
				int wiError = dsr.getWorkItemsProcessingError();
				String details = "id:"+id+" "+"state:"+state+" "+"threads:"+threadCount+" "+"wi.fecth:"+wiFetch+" "+"wi.completed:"+wiCompleted+" "+"wi.error:"+wiError;
				log(exchange, defaultLogType, exchId, event, details);
				timex(exchange, exchId, event);
			}
			else {
				String state = null;
				String details = "id:"+id+" "+"state:"+state;
				log(exchange, LogType.Warn, exchId, event, details);
				timex(exchange, exchId, event);
			}
		}
	}
	
	private void processReceive(Exchange exchange, NodeInventoryUpdateDuccEvent duccEvent) {
		String exchId = exchange.getExchangeId();
		String event = duccEvent.getClass().getSimpleName();
		HashMap<DuccId, IDuccProcess> map = duccEvent.getProcesses();
		if(map != null) {
			Iterator<Entry<DuccId, IDuccProcess>> iterator = map.entrySet().iterator();
			if(iterator.hasNext()) {
				Entry<DuccId, IDuccProcess> entry = iterator.next();
				IDuccProcess jp = entry.getValue();
				NodeIdentity nodeIdentity = jp.getNodeIdentity();
				String ip = nodeIdentity.getIp();
				String name = nodeIdentity.getName();
				String details = "ip:"+ip+" "+"name:"+name+" "+"jp-count:"+map.size();
				log(exchange, defaultLogType, exchId, event, details);
				timex(exchange, exchId, event);
			}
			else {
				log(exchange, LogType.Warn, exchId, event, "empty map?");
				timex(exchange, exchId, event);
			}
		}
		else {
			log(exchange, LogType.Warn, exchId, event, "null map?");
			timex(exchange, exchId, event);
		}
	}
	
	private void processSend(Exchange exchange) throws Exception {
		String exchId = null;
		String event = xclass.getSimpleName();
		String details = null;
		if(exchange != null) {
			if(exchange.getIn() != null) {
				Object body = exchange.getIn().getBody();
				if(body != null) {
					if(body instanceof OrchestratorStateDuccEvent) {
						OrchestratorStateDuccEvent duccEvent = (OrchestratorStateDuccEvent)body;
						int countJ = duccEvent.getWorkMap().getJobCount();
						int countR = duccEvent.getWorkMap().getReservationCount();
						int countS = duccEvent.getWorkMap().getServiceCount();
						details = "jobs:"+countJ+" "+"reservations:"+countR+" "+"services:"+countS;
					}
					if(body instanceof OrchestratorAbbreviatedStateDuccEvent) {
						OrchestratorAbbreviatedStateDuccEvent duccEvent = (OrchestratorAbbreviatedStateDuccEvent)body;
						int countJ = duccEvent.getWorkMap().getJobCount();
						int countR = duccEvent.getWorkMap().getReservationCount();
						int countS = duccEvent.getWorkMap().getServiceCount();
						details = "jobs:"+countJ+" "+"reservations:"+countR+" "+"services:"+countS;
					}
				}
			}
		}
		log(exchange, defaultLogType, exchId, event, details);
		timex(exchange, exchId, event);
	}
	
	private void processReply(Exchange exchange) throws Exception {
		String exchId = null;
		Object body = exchange.getIn().getBody();
		String event = body.getClass().getSimpleName();
		log(exchange, defaultLogType, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void processUnexpected(Exchange exchange, DuccEvent duccEvent) {
		String exchId = null;
		String event = null;
		if(duccEvent != null) {
			event = duccEvent.getClass().getSimpleName();
		}
		log(exchange, LogType.Warn, exchId, event);
		timex(exchange, exchId, event);
	}
	
	private void log(Exchange exchange, LogType logType, String exchId, String event) {
		log(exchange, defaultLogType, exchId, event, null);
	}
	
	private boolean trSequence = true;
	private boolean trLifeStatus = true;
	private boolean trExchId = false;
	private boolean trEvent = true;
	private boolean trDetails = true;
	
	private String log( Exchange exchange, LogType logType, String exchId, String event, String details) {
		String location = "log";
		StringBuffer msgBuf = new StringBuffer();
		if(trSequence) {
			msgBuf.append("seq:"+getSequence(exchange)+" ");
		}
		if(trLifeStatus) {
			msgBuf.append("life:"+lifeStatus+" ");
		}
		if(trExchId) {
			msgBuf.append("exchId:"+exchId+" ");
		}
		if(trEvent) {
			msgBuf.append("event:"+event+" ");
		}
		if(trDetails) {
			if(details != null) {
				msgBuf.append(details+" ");
			}
		}
		String message = msgBuf.toString().trim();
		switch(logType ) {
			case Warn:
				logger.warn(location, duccId, message);
				break;
			case Error:
				logger.error(location, duccId, message);
				break;
			case Info:
				logger.info(location, duccId, message);
				break;
			case Debug:
				logger.debug(location, duccId, message);
				break;
		}
		return message;
	}
	
}
