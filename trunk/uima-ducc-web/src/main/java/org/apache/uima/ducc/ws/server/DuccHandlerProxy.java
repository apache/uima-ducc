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
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.server.IWebMonitor.MonitorType;
import org.eclipse.jetty.server.Request;

import com.google.gson.Gson;

public class DuccHandlerProxy extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccHandlerProxy.class);
	private static DuccId jobid = null;

	public final String proxyJobStatus			= duccContextProxy+"-job-status";
	public final String proxyJobMonitorReport	= duccContextProxy+"-job-monitor-report";
	
	public final String proxyReservationStatus			= duccContextProxy+"-reservation-status";
	public final String proxyReservationMonitorReport	= duccContextProxy+"-reservation-monitor-report";
	
	public final String proxyManagedReservationStatus			= duccContextProxy+"-managed-reservation-status";
	public final String proxyManagedReservationMonitorReport	= duccContextProxy+"-managed-reservation-monitor-report";
	
	DuccWebMonitor duccWebMonitor = DuccWebMonitor.getInstance();

	private boolean isIdMissing(String id) {
		boolean retVal = false;
		if(id.length() == 0) {
			retVal = true;
		}
		return retVal;
	}
	
	private boolean isIdInvalid(String id) {
		boolean retVal = false;
		try {
			int value = Integer.parseInt(id);
			if(value < 0) {
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
		
		if(isIdMissing(jobId)) {
			String message = "id missing";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else if(isIdInvalid(jobId)) {
			String message = "id invalid";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else {
			MonitorInfo monitorInfo  = duccWebMonitor.renew(MonitorType.Job, jobId);
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
		
		ConcurrentHashMap<DuccId,Long> eMap = duccWebMonitor.getExpiryMap(MonitorType.Job);
		
		Gson gson = new Gson();
		String jSon = gson.toJson(eMap);
		duccLogger.debug(location, jobid, jSon);
		response.getWriter().println(jSon);
		response.setContentType("application/json");
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleServletReservationStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletReservationStatus";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());

		String id = request.getParameter("id");
		
		if(id != null) {
			id = id.trim();
		}
		else {
			id = "";
		}
		
		if(isIdMissing(id)) {
			String message = "id missing";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else if(isIdInvalid(id)) {
			String message = "id invalid";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else {
			MonitorInfo monitorInfo  = duccWebMonitor.renew(MonitorType.UnmanagedReservation, id);
			Gson gson = new Gson();
			String jSon = gson.toJson(monitorInfo);
			duccLogger.debug(location, jobid, jSon);
			response.getWriter().println(jSon);
			response.setContentType("application/json");
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleServletReservationMonitorReport(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
			throws IOException, ServletException
	{
		String location = "handleServletReservationMonitorReport";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());

		ConcurrentHashMap<DuccId,Long> eMap = duccWebMonitor.getExpiryMap(MonitorType.UnmanagedReservation);
		
		Gson gson = new Gson();
		String jSon = gson.toJson(eMap);
		duccLogger.debug(location, jobid, jSon);
		response.getWriter().println(jSon);
		response.setContentType("application/json");
		duccLogger.trace(location, jobid, "exit");
	}
			
	private void handleServletManagedReservationStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletManagedReservationStatus";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());

		String id = request.getParameter("id");
		
		if(id != null) {
			id = id.trim();
		}
		else {
			id = "";
		}
		
		if(isIdMissing(id)) {
			String message = "id missing";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else if(isIdInvalid(id)) {
			String message = "id invalid";
			duccLogger.info(location, jobid, message);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
		}
		else {
			MonitorInfo monitorInfo  = duccWebMonitor.renew(MonitorType.ManagedReservation, id);
			Gson gson = new Gson();
			String jSon = gson.toJson(monitorInfo);
			duccLogger.debug(location, jobid, jSon);
			response.getWriter().println(jSon);
			response.setContentType("application/json");
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void handleServletManagedReservationMonitorReport(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleServletManagedReservationMonitorReport";
		duccLogger.trace(location, jobid, "enter");
		duccLogger.info(location, jobid, request.toString());

		ConcurrentHashMap<DuccId,Long> eMap = duccWebMonitor.getExpiryMap(MonitorType.ManagedReservation);
		
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
		else if(reqURI.startsWith(proxyManagedReservationStatus)) {
			handleServletManagedReservationStatus(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(proxyManagedReservationMonitorReport)) {
			handleServletManagedReservationMonitorReport(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(proxyReservationStatus)) {
			handleServletReservationStatus(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(proxyReservationMonitorReport)) {
			handleServletReservationMonitorReport(target, baseRequest, request, response);
		}
		else {
			handleServletUnknown(target, baseRequest, request, response);
		}
		duccLogger.trace(location, jobid, "exit");
	}
	
	
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
			if(isIgnorable(t)) {
				duccLogger.debug(location, jobid, t);
			}
			else {
				duccLogger.info(location, jobid, "", t.getMessage(), t);
				duccLogger.error(location, jobid, t);
			}
		}
	}

}
