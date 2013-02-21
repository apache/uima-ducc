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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.api.DuccMessage;
import org.apache.uima.ducc.api.IDuccMessageProcessor;
import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;

import com.google.gson.Gson;

/**
 * Monitor a DUCC job
 */

public class DuccJobMonitor extends DuccUi {
private Thread main = null;
	
	private static final int RC_UNKNOWN = -1;
	private static final int RC_SUCCESS = 0;
	private static final int RC_FAILURE = 1;
	
	private static final int RC_HELP = RC_SUCCESS;
	
	private static final String NotFound = "NotFound";
	
	private static final String StateRunning 	= "Running";
	private static final String StateCompleting = "Completing";
	private static final String StateCompleted 	= "Completed";
	
	private AtomicInteger rc = new AtomicInteger(RC_UNKNOWN);
	private AtomicBoolean cancel_job_on_interrupt = new AtomicBoolean(false);
	
	private String jobId = null;
	
	private boolean info = true;
	private boolean error = true;
	private boolean debug = false;
	
	private boolean timestamp = false;
	
	private int milliseconds 	= 1;
	private int seconds		 	= 1000*milliseconds;
	private int wakeupInterval 	= 15*seconds;
	
	private int urlTimeout = 60*seconds;
	
	private IDuccMessageProcessor duccMessageProcessor = new DuccMessage();
	
	DuccPropertiesResolver duccPropertiesResolver;

	private void debug(String message) {
		if(debug) {
			duccMessageProcessor.out(timestamp(message));
		}
	}
	
	private void debug(Exception e) {
		if(debug) {
			duccMessageProcessor.exception(e);
		}
	}
	
	private void info(String message) {
		if(info) {
			duccMessageProcessor.out(timestamp(message));
		}
	}
	
	private void error(String message) {
		if(error) {
			duccMessageProcessor.out(timestamp(message));
		}
	}
	
	private String timestamp(String message) {
		String tMessage = message;
		if(timestamp) {
			String date = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new java.util.Date());
			tMessage = date+" "+message;
		}
		return tMessage;
	}
	
	public DuccJobMonitor() {
	}
	
	public DuccJobMonitor(IDuccMessageProcessor duccMessageProcessor) {
		this.duccMessageProcessor = duccMessageProcessor;
	}
	
	@SuppressWarnings("static-access")
	private void addOptions(Options options) {
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_help).hasArg(false)
				.withLongOpt(DuccUiConstants.name_help).create());
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_debug).hasArg(false)
				.withLongOpt(DuccUiConstants.name_debug).create());
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_timestamp).hasArg(false)
				.withLongOpt(DuccUiConstants.name_timestamp).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_job_id)
				.withDescription(makeDesc(DuccUiConstants.desc_job_id,DuccUiConstants.exmp_job_id)).hasArg()
				.withLongOpt(DuccUiConstants.name_job_id).create());
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_monitor_cancel_job_on_interrupt).hasArg(false)
				.withLongOpt(DuccUiConstants.name_monitor_cancel_job_on_interrupt).create());
	}
	
	protected void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccJobMonitor.class.getName(), options);
		rc.set(RC_HELP);
		return;
	}
	
	private String getSingleLineStatus(String urlString) {
		String line = null;
		URL url = null;
		try {
		    url = new URL(urlString);
		    URLConnection uc = url.openConnection();
		    uc.setReadTimeout(urlTimeout); 
		    BufferedReader br = new BufferedReader(new InputStreamReader(uc.getInputStream()));
		    line = br.readLine();
		    br.close();
		} 
		catch (MalformedURLException e) {
		    e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		} 
		return line;
	}
	
	private String getUrlString(String id) {
		String host = duccPropertiesResolver.getFileProperty("ducc.ws.node");
		if(host == null) {
			host = duccPropertiesResolver.getFileProperty("ducc.head");
		}
		String port = duccPropertiesResolver.getFileProperty("ducc.ws.port");
		String urlString = "http://"+host+":"+port+"/ducc-servlet/proxy-job-status?id="+id;
		debug(urlString);
		return urlString;
	}
	
	private void adjustWakeupInterval() {
		String rate = duccPropertiesResolver.getFileProperty("ducc.orchestrator.state.publish.rate");
		try {
			wakeupInterval = Integer.parseInt(rate);
		}
		catch(Exception e) {
			debug(e);
		}
	}
	
	private String details(MonitorInfo monitorInfo) {
		StringBuffer sb = new StringBuffer();
		sb.append(" ");
		sb.append("total:");
		sb.append(monitorInfo.total);
		sb.append(" ");
		sb.append("done:");
		sb.append(monitorInfo.done);
		sb.append(" ");
		sb.append("error:");
		sb.append(monitorInfo.error);
		sb.append(" ");
		sb.append("retry:");
		sb.append(monitorInfo.retry);
		sb.append(" ");
		sb.append("procs:");
		sb.append(monitorInfo.procs);
		return sb.toString();
	}
	
	public int run(String[] args) throws Exception {
		/*
		 * require DUCC_HOME 
		 */
		String ducc_home = Utils.findDuccHome();
		if(ducc_home == null) {
			duccMessageProcessor.err("missing required environment variable: DUCC_HOME");
			rc.set(RC_FAILURE);
			return rc.get();
		}
		/*
		 * parser is not thread safe?
		 */
		synchronized(DuccUi.class) {
			Options options = new Options();
			addOptions(options);
			CommandLineParser parser = new PosixParser();
			CommandLine commandLine = parser.parse(options, args);
			/*
			 * give help & exit when requested
			 */
			if (commandLine.hasOption(DuccUiConstants.name_help)) {
				help(options);
				return rc.get();
			}
			if(commandLine.getOptions().length == 0) {
				help(options);
				return rc.get();
			}
			/*
			 * detect duplicate options
			 */
			if (DuccUiUtilities.duplicate_options(duccMessageProcessor, commandLine)) {
				return 1;
			}
			/*
			 * timestamp
			 */
			if (commandLine.hasOption(DuccUiConstants.name_timestamp)) {
				timestamp = true;
			}
			/*
			 * verbosity
			 */
			if (commandLine.hasOption(DuccUiConstants.name_debug)) {
				debug = true;
			}
			/*
			 * cancel job enabled
			 */
			if (commandLine.hasOption(DuccUiConstants.name_monitor_cancel_job_on_interrupt)) {
				cancel_job_on_interrupt.set(true);
			}
			/*
			 * job id
			 */
			if (commandLine.hasOption(DuccUiConstants.name_job_id)) {
				jobId = commandLine.getOptionValue(DuccUiConstants.name_job_id);
			}
			else {
				HelpFormatter formatter = new HelpFormatter();
				formatter.setWidth(110);
				formatter.printHelp(DuccJobMonitor.class.getName(), options);
				rc.set(RC_HELP);
				return rc.get();
			}
		}
		
		main = Thread.currentThread();
		
		Thread killer = new Killer(main);
		Runtime.getRuntime().addShutdownHook(killer);
		
		duccPropertiesResolver = DuccPropertiesResolver.getInstance();
		
		adjustWakeupInterval();
		
		boolean observer = true;
		String urlString = getUrlString(jobId);
		String lastMessage = "";
		String thisMessage = "";
		
		StringBuffer message = new StringBuffer();
		
		message.append("id:"+jobId);
		message.append(" ");
		message.append("location:");
		message.append(ManagementFactory.getRuntimeMXBean().getName());
		info(message.toString());
		
		while(observer) {
			
			String json = getSingleLineStatus(urlString);
			debug(json);
			
			if(json != null) {
				Gson gson = new Gson();
				MonitorInfo monitorInfo = gson.fromJson(json, MonitorInfo.class);

				int stateCount = monitorInfo.stateSequence.size();
				debug("states:"+stateCount);
				if(stateCount <= 0) {
					message = new StringBuffer();
					message.append("id:"+jobId);
					message.append(" ");
					message.append("state:"+NotFound);
					thisMessage = message.toString();
					info(thisMessage);
					return rc.get();
				}
				
				String state = "";
				Iterator<String> states = monitorInfo.stateSequence.iterator();
				while(states.hasNext()) {
					state = states.next();
				}
				
				message = new StringBuffer();
				message.append("id:"+jobId);
				message.append(" ");
				message.append("state:"+state);
				
				if(state.equals(StateRunning)) {
					message.append(details(monitorInfo));
				}
				else if(state.equals(StateCompleting)) {
					cancel_job_on_interrupt.set(false);
					message.append(details(monitorInfo));
				}
				else if(state.equals(StateCompleted)) {
					cancel_job_on_interrupt.set(false);
					message.append(details(monitorInfo));
				}
				
				thisMessage = message.toString();
				if(!thisMessage.equals(lastMessage)) {
					info(thisMessage);
					lastMessage = thisMessage;
				}
				
				if(state.equals(StateCompleted)) {
					if(monitorInfo.procs.equals("0")) {
						if(monitorInfo.total.equals(monitorInfo.done)) {
							message = new StringBuffer();
							message.append("id:"+jobId);
							message.append(" ");
							message.append("rc:"+RC_SUCCESS);
							thisMessage = message.toString();
							info(thisMessage);
							rc.set(RC_SUCCESS);
							return rc.get();
						}
						else {
							message = new StringBuffer();
							message.append("id:"+jobId);
							message.append(" ");
							message.append("rc:"+RC_FAILURE);
							thisMessage = message.toString();
							info(thisMessage);
							rc.set(RC_FAILURE);
							return rc.get();
						}
					}
				}
			}
			else {
				error("error: timeout accessing "+urlString);
			}

			try {
				Thread.sleep(wakeupInterval);
			} catch (InterruptedException e) {
				debug(e);
			}
		}
		
		return rc.get();
	}
	
	private class Killer extends Thread {
		
		public Killer(Thread thread) {
		}
		
		public void run() {
			StringBuffer message = new StringBuffer();
			if(cancel_job_on_interrupt.get()) {
				message.append("killer: cancel");
				cancel();
			}
			else {
				message.append("killer: no cancel");
			}
			debug(message.toString());
		}
	}
	
	public void cancel() {
       	try {
       		ArrayList<String> arrayList = new ArrayList<String>();
       		arrayList.add("--"+DuccUiConstants.name_job_id);
       		arrayList.add(jobId);
       		arrayList.add("--"+DuccUiConstants.name_reason);
       		arrayList.add("\"submitter was terminated via interrupt\"");
       		String[] argList = arrayList.toArray(new String[0]);
    		DuccJobCancel duccJobCancel = new DuccJobCancel();
    		int retVal = duccJobCancel.run(argList);
    		if(retVal == 0) {
    		}
    		else {
    		}
    	} catch (Exception e) {
    		duccMessageProcessor.exception(e);
    	}
	}
	public static void main(String[] args) {
		try {
			DuccJobMonitor duccJobMonitor = new DuccJobMonitor();
			int rc = duccJobMonitor.run(args);
            System.exit(rc == 0 ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(1);
		}
		return;
	}

}
