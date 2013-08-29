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
package org.apache.uima.ducc.cli;

import java.util.ArrayList;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;

public class DuccJobMonitor extends DuccMonitor implements IDuccMonitor {

	public static String servlet = "/ducc-servlet/proxy-job-status";
	
	public DuccJobMonitor() {
		super(DuccContext.Job, true);
	}
	
	public DuccJobMonitor(IDuccCallback messageProcessor) {
		super(DuccContext.Job, true, messageProcessor);
	}
	
	private DuccJobMonitor(String uniqueSignature) {
		super(DuccContext.Job, false);
	}

	public static void main(String[] args) {
		int code = RC_FAILURE;
		try {
			String unique = null;
			DuccJobMonitor jobMonitor = new DuccJobMonitor(unique);
			code = jobMonitor.run(args);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(code);
	}
	
	@Override
	public void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccJobMonitor.class.getName(), options);
		return;
	}
	
	@Override
	public void cancel() {
		try {
       		ArrayList<String> arrayList = new ArrayList<String>();
       		arrayList.add("--"+UiOption.JobId.pname());
       		arrayList.add(getId());
       		arrayList.add("--"+UiOption.Reason.pname());
       		arrayList.add("\"submitter was terminated via interrupt\"");
       		String[] argList = arrayList.toArray(new String[0]);
    		DuccJobCancel jobCancel = new DuccJobCancel(argList);
    		boolean retVal = jobCancel.execute();
    		debug("cancel rc:"+retVal);
    	} catch (Exception e) {
    		messageProcessor.status(e.toString());
    	}
	}

	@Override
	public String getUrl(String id) {
		String urlString = "http://"+getHost()+":"+getPort()+servlet+"?id="+id;
		debug(urlString);
		return urlString;
	}
}
