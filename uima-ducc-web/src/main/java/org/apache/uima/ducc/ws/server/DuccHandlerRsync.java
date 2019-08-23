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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.eclipse.jetty.server.Request;

public class DuccHandlerRsync extends DuccAbstractHandler {

	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccHandlerRsync.class);
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;
	
	private String duccinator_update = duccContext+"/duccinator-update";
	
	public DuccHandlerRsync() {
	}

	/*
	 * join lhs + rhs with single separator
	 */
	private String joiner(String lhs,String rhs) {
		String retVal = lhs;
		if(lhs != null) {
			if(rhs != null) {
				retVal = lhs+File.separator+rhs;
				retVal = retVal.replaceAll("/+","/");
			}
		}
		return retVal;
	}
	
	/*
	 * produce rsync command line string
	 */
	private String getCmdRsync() {
		String ducc_home = IDuccEnv.DUCC_HOME_DIR;
		String cmd = "admin"+File.separator+"ducc_rsync";
		String retVal = joiner(ducc_home,cmd);
		return retVal;
	}
	
	/*
	 * run /<ducc_home>/admin/ducc_rsync --agent-nodes <node>
	 */
	private String runCmdRsync(String node) {
		String location = "runCmdRsync";
		String retVal = null;
		try {
			List<String> pb_command = new ArrayList<String>();
			pb_command.add(getCmdRsync());
			pb_command.add("--agent-nodes");
			pb_command.add(node);
			duccLogger.info(location, jobid, pb_command);
			ProcessBuilder pb = new ProcessBuilder( pb_command );
			Process p = pb.start();
			InputStream pOut = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(pOut);
			BufferedReader br = new BufferedReader(isr);
	        String line;
	        StringBuffer sb = new StringBuffer();
	        while ((line = br.readLine()) != null) {
	        	sb.append(line);
	        	duccLogger.info(location, jobid, line);
	        }
	        retVal = sb.toString();
	        int rc = p.waitFor();
	        duccLogger.info(location, jobid, rc);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	/*
	 * produce start command line string
	 */
	private String getCmdStart() {
		String ducc_home = IDuccEnv.DUCC_HOME_DIR;
		String cmd = "admin"+File.separator+"start_ducc";
		String retVal = joiner(ducc_home,cmd);
		return retVal;
	}

	/*
	 * run /<ducc_home>/admin/start_ducc -c agent@<node>
	 */
	private String runCmdStart(String node) {
		String location = "runCmdStart";
		String retVal = null;
		try {
			List<String> pb_command = new ArrayList<String>();
			pb_command.add(getCmdStart());
			pb_command.add("-c");
			pb_command.add("agent@"+node);
			duccLogger.info(location, jobid, pb_command);
			ProcessBuilder pb = new ProcessBuilder( pb_command );
			Process p = pb.start();
			InputStream pOut = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(pOut);
			BufferedReader br = new BufferedReader(isr);
	        String line;
	        StringBuffer sb = new StringBuffer();
	        while ((line = br.readLine()) != null) {
	        	sb.append(line);
	        	duccLogger.info(location, jobid, line);
	        }
	        retVal = sb.toString();
	        int rc = p.waitFor();
	        duccLogger.info(location, jobid, rc);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private void handleDuccServletDuccinatorUpdate(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletDuccinatorUpdate";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String node = request.getRemoteHost();
		String result;;
		result = runCmdRsync(node);
		sb.append(result);
		result = runCmdStart(node);
		sb.append(result);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws Exception
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		duccLogger.debug(methodName, null,request.toString());
		duccLogger.debug(methodName, null,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(duccContext)) {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			if(reqURI.startsWith(duccinator_update)) {
				handleDuccServletDuccinatorUpdate(target, baseRequest, request, response);
				//DuccWebUtil.noCache(response);
			}
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException {
		String methodName = "handle";
		try{
			handleDuccRequest(target, baseRequest, request, response);
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
