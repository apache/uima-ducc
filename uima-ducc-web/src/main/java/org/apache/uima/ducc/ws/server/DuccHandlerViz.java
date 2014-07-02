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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.server.nodeviz.NodeViz;
import org.eclipse.jetty.server.Request;

public class DuccHandlerViz extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerClassic.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;
	
	public final String vizNodes 				= duccContextViz+"-nodes";
	
	NodeViz viz = null;
	
	DuccHandlerViz()
	{
		viz = new NodeViz();
	}
	
	private void handleServletVizNodes(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletVizNodes";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		
		//String data = "<html><p>"+methodName+" not yet implemented</p></html>";
		String data = viz.getVisualization();
		
		duccLogger.debug(methodName, jobid, data);
		response.getWriter().println(data);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	
	private void handleServletUnknown(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletUnknown";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.info(methodName, jobid, request.toString());
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.debug(methodName, jobid,request.toString());
		duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(vizNodes)) {
			handleServletVizNodes(target, baseRequest, request, response);
		}
		else {
			handleServletUnknown(target, baseRequest, request, response);
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws IOException, ServletException {
		String methodName = "handle";
		try { 
			duccLogger.debug(methodName, jobid,request.toString());
			duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
			String reqURI = request.getRequestURI()+"";
			if(reqURI.startsWith(duccContextViz)) {
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				handleDuccRequest(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
		}
		catch(Throwable t) {
			if(isIgnorable(t)) {
				duccLogger.debug(methodName, jobid, t);
			}
			else {
				duccLogger.info(methodName, jobid, "", t.getMessage(), t);
				duccLogger.error(methodName, jobid, t);
			}
		}
	}
}
