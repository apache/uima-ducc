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
package org.apache.uima.ducc.ws.handlers.experiments;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.handlers.utilities.HandlersUtilities;
import org.apache.uima.ducc.ws.log.WsLog;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;
import org.apache.uima.ducc.ws.server.DuccLocalConstants;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.eclipse.jetty.server.handler.AbstractHandler;

public abstract class HandlerExperimentsAbstract extends AbstractHandler {

  // NOTE - this variable used to hold the class name before WsLog was simplified
	private static DuccLogger cName = DuccLogger.getLogger(HandlerExperimentsAbstract.class);
	
	private static DuccData duccData = DuccData.getInstance();

	public final String duccContext = DuccLocalConstants.duccContext;

	protected DuccWebServer duccWebServer = null;

	public void init(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
	}

	public boolean isIgnorable(Throwable t) {
		return HandlersUtilities.isIgnorable(t);
	}

	public String getFmtDuration(IDuccWorkJob job, HttpServletRequest request, long now) {
		String retVal = "";
		StringBuffer tb = new StringBuffer();
		if(job.isCompleted()) {
			String duration = getDuration(request,job);
			String decoratedDuration = decorateDuration(request,job, duration);
			tb.append("<span>");
			tb.append(decoratedDuration);
			tb.append("</span>");
			retVal = tb.toString();
		}
		else {
			String duration = getDuration(request,job,now);
			String decoratedDuration = decorateDuration(request,job, duration);
			tb.append("<span class=\"health_green\""+">");
			tb.append(decoratedDuration);
			tb.append("</span>");
			String projection = getProjection(request,job);
			tb.append(projection);
			retVal = tb.toString();
		}
		return retVal;
	}

	public String getLink(IDuccWorkJob job) {
		String retVal = "";
		String parm = job.getDuccId().toString();
		String displayId =  "j"+parm;
		String link = "<a href=\"job.details.html?id="+parm+"\">"+displayId+"</a>";
		retVal = link;
		return retVal;
	}

	public String getTotal(IDuccWorkJob job) {
		String retVal = "";
		String total = job.getSchedulingInfo().getWorkItemsTotal();
		retVal = total;
		return retVal;
	}

	public String getDone(IDuccWorkJob job) {
		String retVal = "";
		String done = job.getSchedulingInfo().getWorkItemsCompleted();
		IDuccPerWorkItemStatistics perWorkItemStatistics = job.getSchedulingInfo().getPerWorkItemStatistics();
		if (perWorkItemStatistics != null) {
			double max = Math.round(perWorkItemStatistics.getMax()/100.0)/10.0;
			double min = Math.round(perWorkItemStatistics.getMin()/100.0)/10.0;
			double avg = Math.round(perWorkItemStatistics.getMean()/100.0)/10.0;
			double dev = Math.round(perWorkItemStatistics.getStandardDeviation()/100.0)/10.0;
			done = "<span title=\""+"seconds-per-work-item "+"Max:"+max+" "+"Min:"+min+" "+"Avg:"+avg+" "+"Dev:"+dev+"\""+">"+done+"</span>";
		}
		retVal = done;
		return retVal;
	}

	public String getDispatch(IDuccWorkJob job) {
		String retVal = "";
		if(duccData.isLive(job.getDuccId())) {
			int unassigned = job.getSchedulingInfo().getCasQueuedMap().size();
			try {
				int dispatched = Integer.parseInt(job.getSchedulingInfo().getWorkItemsDispatched())-unassigned;
				retVal = ""+dispatched;
			}
			catch(Exception e) {
			}
		}
		else {
			retVal = "0";
		}
		return retVal;
	}

	public String getError(IDuccWorkJob job) {
		String retVal = "";
		retVal = buildErrorLink(job);
		return retVal;
	}

	public String getRetry(IDuccWorkJob job) {
		String retVal = "";
		retVal = job.getSchedulingInfo().getWorkItemsRetry();
		return retVal;
	}

	public String getPreempt(IDuccWorkJob job) {
		String retVal = "";
		retVal = job.getSchedulingInfo().getWorkItemsPreempt();
		return retVal;
	}

	public String buildErrorLink(IDuccWorkJob job) {
		return(buildErrorLink(job,null));
	}

	public final String duccLogData			  = duccContext+"/log-data";

	public String buildErrorLink(IDuccWorkJob job, String name) {
		String retVal = job.getSchedulingInfo().getWorkItemsError();
		if(!retVal.equals("0")) {
			String errorCount = retVal;
			if(name == null) {
				name = errorCount;
			}
			String logfile = job.getUserLogDir() + "jd.err.log";
			String href = "<a href=\""+duccLogData+"?"+"fname="+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+name+"</a>";
			retVal = href;
		}
		return retVal;
	}

	public String getDuration(HttpServletRequest request, IDuccWork dw) {
		String mName = "getDuration";
		String retVal = "";
		try {
			String v2 = dw.getStandardInfo().getDateOfCompletion();
			String v1 = dw.getStandardInfo().getDateOfSubmission();
			WsLog.trace(cName, mName, "v2:"+v2+" v1:"+v1);
			retVal = getDuration(dw.getDuccId(),v2,v1);
		}
		catch(Exception e) {
			//
		}
		catch(Throwable t) {
			//
		}
		return retVal;
	}

	public String getDuration(HttpServletRequest request, IDuccWork dw, long now) {
		String retVal = "";
		try {
			String v2 = ""+now;
			String v1 = dw.getStandardInfo().getDateOfSubmission();
			retVal = getDuration(dw.getDuccId(),v2,v1);
		}
		catch(Exception e) {
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	public String getDuration(DuccId jobId, String millisV2, String millisV1) {
		String retVal = "";
		try {
			long d2 = Long.parseLong(millisV2);
			long d1 = Long.parseLong(millisV1);
			long diff = d2 - d1;
			retVal = FormatHelper.duration(diff,Precision.Whole);
		}
		catch(Exception e) {
			//
		}
		catch(Throwable t) {
			//
		}
		return retVal;
	}

	public String decorateDuration(HttpServletRequest request, IDuccWorkJob job, String duration) {
		String mName = "decorateDuration";
		String retVal = duration;
		try {
			StringBuffer title = new StringBuffer();
			double avgMillisPerWorkItem = getAvgMillisPerWorkItem(request, job);
			if(avgMillisPerWorkItem > 0) {
				if(avgMillisPerWorkItem < 500) {
					avgMillisPerWorkItem = 500;
				}
			}
			int iAvgMillisPerWorkItem = (int)avgMillisPerWorkItem;
			if(iAvgMillisPerWorkItem > 0) {
				if(title.length() > 0) {
					title.append("; ");
				}
				title.append("Time (ddd:hh:mm:ss) elapsed for job, average processing time per work item="+FormatHelper.duration(iAvgMillisPerWorkItem,Precision.Whole));
			}
			String cVal = getCompletion(request,job);
			if(cVal != null) {
				if(cVal.length() > 0) {
					if(title.length() > 0) {
						title.append("; ");
					}
					title.append("End="+cVal);
				}
			}
			if(title.length() > 0) {
				retVal = "<span "+"title=\""+title+"\""+">"+duration+"</span>";
			}
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
		return retVal;
	}

	public String decorateDuration(HttpServletRequest request, IDuccWorkReservation reservation, String duration) {
		String retVal = duration;
		String cVal = getCompletion(request,reservation);
		if(cVal != null) {
			if(cVal.length() > 0) {
				String title = "title=\""+"End="+cVal+"\"";
				retVal = "<span "+title+">"+duration+"</span>";
			}
		}
		return retVal;
	}


	public double getAvgMillisPerWorkItem(HttpServletRequest request, IDuccWorkJob job) {
		double avgMillis = 0;
		IDuccSchedulingInfo schedulingInfo = job.getSchedulingInfo();
		IDuccPerWorkItemStatistics perWorkItemStatistics = schedulingInfo.getPerWorkItemStatistics();
		if (perWorkItemStatistics != null) {
			avgMillis = perWorkItemStatistics.getMean();
		}
		return avgMillis;
}

	public String getCompletion(HttpServletRequest request, IDuccWorkJob job) {
		String retVal = "";
		try {
			String tVal = job.getStandardInfo().getDateOfCompletion();
			retVal = getTimeStamp(request,job.getDuccId(),tVal);
		}
		catch(Exception e) {
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	public String getCompletion(HttpServletRequest request, IDuccWorkReservation reservation) {
		String retVal = "";
		try {
			String tVal = reservation.getStandardInfo().getDateOfCompletion();
			retVal = getTimeStamp(request,reservation.getDuccId(),tVal);
		}
		catch(Exception e) {
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	public String getTimeStamp(HttpServletRequest request, DuccId jobId, String millis) {
		return getTimeStamp(DuccCookies.getDateStyle(request),getTimeStamp(jobId, millis));
	}

	private String getTimeStamp(DuccId jobId, String millis) {
		String mName = "getTimeStamp";
		String retVal = "";
		try {
			retVal = TimeStamp.simpleFormat(millis);
		}
		catch(Throwable t) {
			WsLog.debug(cName, mName, "millis:"+millis);
		}
		return retVal;
	}

	public String getTimeStamp(DateStyle dateStyle, String date) {
		String mName = "getTimeStamp";
		StringBuffer sb = new StringBuffer();
		if(date != null) {
			sb.append(date);
			if(date.trim().length() > 0) {
				try {
					switch(dateStyle) {
					case Long:
						break;
					case Medium:
						String day = sb.substring(sb.length()-4);
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						sb.append(day);
						break;
					case Short:
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						break;
					}
				}
				catch(Exception e) {
					 WsLog.error(cName, mName, e);
				}
			}
		}
		return sb.toString();
	}

	public String getProjection(HttpServletRequest request, IDuccWorkJob job) {
		String mName = "getProjection";
		String retVal = "";
		try {
			IDuccSchedulingInfo schedulingInfo = job.getSchedulingInfo();
			IDuccPerWorkItemStatistics perWorkItemStatistics = schedulingInfo.getPerWorkItemStatistics();
			if (perWorkItemStatistics == null) {
				return "";
			}
			int total = schedulingInfo.getIntWorkItemsTotal();
			int completed = schedulingInfo.getIntWorkItemsCompleted();
			int error = schedulingInfo.getIntWorkItemsError();
			int remainingWorkItems = total - (completed + error);
			if(remainingWorkItems > 0) {
				int usableProcessCount = job.getProcessMap().getUsableProcessCount();
				if(usableProcessCount > 0) {
					if(completed > 0) {
						int threadsPerProcess = schedulingInfo.getIntThreadsPerProcess();
						int totalThreads = usableProcessCount * threadsPerProcess;
						double remainingIterations = remainingWorkItems / totalThreads;
						double avgMillis = perWorkItemStatistics.getMean();
						double leastOperatingMillis = job.getWiMillisOperatingLeast();
						double mostCompletedMillis = job.getWiMillisCompletedMost();
						double projectedTime = (avgMillis * remainingIterations) + (mostCompletedMillis - leastOperatingMillis);
						String text = "avgMillis:"+avgMillis+" "+"remainingIterations:"+remainingIterations+" "+"mostCompleteMillis:"+mostCompletedMillis+" "+"leastOperatingMillis:"+leastOperatingMillis;
						WsLog.trace(cName, mName, text);
						if(projectedTime > 0) {
							long millis = Math.round(projectedTime);
							if(millis > 1000) {
								String projection = FormatHelper.duration(millis,Precision.Whole);
								String health = "class=\"health_yellow\"";
								String title = "title=\"Time (ddd:hh:mm:ss) until projected completion\"";
								retVal = "+"+"<span "+health+" "+title+"><i>"+projection+"</i></span>";
								retVal = " {"+retVal+"}";
							}
						}
						else {
							long millis = Math.round(0-projectedTime);
							if(millis > 1000) {
								String projection = FormatHelper.duration(millis,Precision.Whole);
								String health = "class=\"health_purple\"";
								String title = "title=\"Time (ddd:hh:mm:ss) past-due projected completion\"";
								retVal = "-"+"<span "+health+" "+title+"><i>"+projection+"</i></span>";
								retVal = " {"+retVal+"}";
							}
						}
					}
				}
			}
		}
		catch(Throwable t) {
			WsLog.error(cName, mName, t);
		}
		return retVal;
	}
}
