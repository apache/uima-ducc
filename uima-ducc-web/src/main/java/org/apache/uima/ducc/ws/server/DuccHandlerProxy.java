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
package org.apache.uima.ducc.ws.server;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.eclipse.jetty.server.Request;

import com.google.gson.Gson;

public class DuccHandlerProxy extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerProxy.class.getName());
	private static DuccId jobid = null;

	public final String proxyJobStatus			= duccContextProxy+"-job-status";
	public final String proxyJobMonitorReport	= duccContextProxy+"-job-monitor-report";
	
	DuccWebMonitor duccWebMonitor = DuccWebMonitor.getInstance();

	private boolean isJobIdMissing(String jobId) {
		boolean retVal = false;
		if(jobId.length() == 0) {
			retVal = true;
		}
		return retVal;
	}
	
	private boolean isJobIdInvalid(String jobId) {
		boolean retVal = false;
		try {
			int id = Integer.parseInt(jobId);
			if(id < 0) {
				retVal = true;
			}
		}
		catch(Exception e) {
			retVal = true;
		}
		return retVal;
	}
	
	private void handleServletJobStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletJobStatus";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());

		String jobId = request.getParameter("id");
		
		if(jobId != null) {
			jobId = jobId.trim();
		}
		else {
			jobId = "";
		}
		
		if(isJobIdMissing(jobId)) {
			String message = "Job id missing";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else if(isJobIdInvalid(jobId)) {
			String message = "Job id invalid";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else {
			MonitorInfo monitorInfo  = duccWebMonitor.renew(jobId);
			Gson gson = new Gson();
			String jSon = gson.toJson(monitorInfo);
			duccLogger.debug(location, jobid, jSon);
			response.getWriter().println(jSon);
			response.setContentType("application/json");
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleServletJobMonitorReport(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletJobMonitorReport";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());
		
		ConcurrentHashMap<DuccId,Long> eMap = duccWebMonitor.getExpiryMap();
		
		Gson gson = new Gson();
		String jSon = gson.toJson(eMap);
		duccLogger.debug(location, jobid, jSon);
		response.getWriter().println(jSon);
		response.setContentType("application/json");
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleServletUnknown(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletUnknown";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleDuccRequest(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleDuccRequest";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.debug(location, jobid, request.toString());
		duccLogger.debug(location, jobid, "getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(proxyJobStatus)) {
			handleServletJobStatus(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(proxyJobMonitorReport)) {
			handleServletJobMonitorReport(target, baseRequest, request, response);
		}
		else {
			handleServletUnknown(target, baseRequest, request, response);
		}
		duccLogger.trace(location, jobid, "exit");
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException {
		String location = "handle";
		try{ 
			duccLogger.debug(location, jobid,request.toString());
			duccLogger.debug(location, jobid,"getRequestURI():"+request.getRequestURI());
			String reqURI = request.getRequestURI()+"";
			if(reqURI.startsWith(duccContextProxy)) {
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				handleDuccRequest(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
		}
		catch(Throwable t) {
			duccLogger.info(location, jobid, "", t.getMessage(), t);
			duccLogger.error(location, jobid, t);
		}
	}

}
