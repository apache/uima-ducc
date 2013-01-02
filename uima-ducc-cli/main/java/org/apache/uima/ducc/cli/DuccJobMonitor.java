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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.api.DuccMessage;
import org.apache.uima.ducc.api.IDuccMessageProcessor;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


/**
 * Monitor a DUCC job
 */

public class DuccJobMonitor extends DuccUi implements DuccEventDelegateListener {
	
	private static final int RC_UNKNOWN = -1;
	private static final int RC_SUCCESS = 0;
	private static final int RC_FAILURE = 1;
	
	private static final int RC_HELP = RC_SUCCESS;
	
	private static final String NotFound = "NotFound";
	
	private AtomicBoolean stopped = new AtomicBoolean(false);
	private AtomicBoolean jobActive = new AtomicBoolean(true);
	private AtomicInteger rc = new AtomicInteger(RC_UNKNOWN);
	
	private String lastMessage = "";
	
	private Thread main = null;
	
	private CamelContext context;
	private ActiveMQComponent amqc;
	
	private String broker = DuccUiUtilities.buildBrokerUrl();
	private String endpoint = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_jms_provider)
    						+ ":"
    						+ DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_state_update_endpoint_type)
    						+ ":"
    						+ DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_state_update_endpoint)
    						;
	private String jmsProvider = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_jms_provider);

	private String jobId = null;
	private DuccWorkJob job = null;
	
	private boolean info = true;
	private boolean debug = false;
	private boolean cancel_job_on_interrupt = false;
	
	private int milliseconds = 1;
	private int seconds		 = 1000*milliseconds;
	private int wakeupTime 	 = 60*seconds;

	private int MAXLINES = 2000;
	
	private IDuccMessageProcessor duccMessageProcessor = new DuccMessage();
	
	private void debug(String message) {
		if(debug) {
			duccMessageProcessor.out(message);
		}
	}
	
	private void debug(Exception e) {
		if(debug) {
			duccMessageProcessor.exception(e);
		}
	}
	
	private void info(String message) {
		if(info) {
			duccMessageProcessor.out(message);
		}
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
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_service_broker)
				.withDescription(makeDesc(DuccUiConstants.desc_service_broker,DuccUiConstants.exmp_service_broker)).hasArg()
				.withLongOpt(DuccUiConstants.name_service_broker).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_service_endpoint)
				.withDescription(makeDesc(DuccUiConstants.desc_service_endpoint,DuccUiConstants.exmp_service_endpoint)).hasArg()
				.withLongOpt(DuccUiConstants.name_service_endpoint).create());
	}
	
	private void start() {
		context = new DefaultCamelContext();
		amqc = ActiveMQComponent.activeMQComponent(broker);
        context.addComponent(jmsProvider, amqc);
        try {
			context.addRoutes(this.routeBuilderForIncomingRequests(endpoint, this));
		} catch (Exception e) {
			duccMessageProcessor.exception(e);
		}
		try {
			context.start();
		} catch (Exception e) {
			duccMessageProcessor.exception(e);
		}
	}
	
	private void stop() {
		while(!stopped.get()) {
			boolean success = stopped.compareAndSet(false, true);
			if(success) {
				try {
					CamelUtil.stop(context);
				} catch (Exception e) {
					duccMessageProcessor.exception(e);
				}
			}
		}
	}
	
	public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final DuccJobMonitor delegate) {
        return new RouteBuilder() {
            public void configure() {
            	//System.out.println("..... Defining Router on endpoint:"+endpoint);
            	from(endpoint)
            	//.unmarshal().xstream()
            	.process(new MonitorProcessor())
            	.bean(delegate);
            }
        };
	}
	
	public class MonitorProcessor implements Processor {
		public void process( Exchange ex ) {
			//System.out.println("..... Monitor received an event ....");
		}
	}
	
	boolean isWorkCompleted(String v1, String v2) {
		boolean retVal = false;
		try {
			int intValue1 = Integer.parseInt(v1);
			int intValue2 = Integer.parseInt(v2);
			if(intValue1 > 0) {
				if(intValue1 == intValue2) {
					retVal = true;
				}
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public void onOrchestratorStateDuccEvent(@Body OrchestratorStateDuccEvent duccEvent) throws Exception {
		synchronized(jobActive) {
			if(jobActive.get()) {
				debug("JobCount:"+duccEvent.getWorkMap().getJobCount());
				job = (DuccWorkJob) duccEvent.getWorkMap().findDuccWork(DuccType.Job, jobId);
				if(job == null) {
					StringBuffer message = new StringBuffer();
					message.append("id:"+jobId);
					message.append(" ");
					message.append("state:"+NotFound);
					info(message.toString());
					jobActive.set(false);
				}
				else {
					JobState jobState = (JobState) job.getStateObject();
					String total = job.getSchedulingInfo().getWorkItemsTotal();
					String completed = job.getSchedulingInfo().getWorkItemsCompleted();
					String error = job.getSchedulingInfo().getWorkItemsError();
					String retry = job.getSchedulingInfo().getWorkItemsRetry();
					StringBuffer messageBuffer = new StringBuffer();
					String message = messageBuffer.toString();
					messageBuffer.append("id:"+jobId);
					messageBuffer.append(" ");
					messageBuffer.append("state:"+jobState);
					switch(jobState) {
					case Completed:
						int count = job.getProcessMap().getAliveProcessCount();
						if(count > 0) {
							messageBuffer.append(" ");
							messageBuffer.append("processes stopping:"+count);
						}
						else {
							jobActive.set(false);
							if(isWorkCompleted(total,completed)) {
								rc.set(RC_SUCCESS);
							}
							else {
								rc.set(RC_FAILURE);
							}
						}
					case Running:
					case Completing:
						messageBuffer.append(" ");
						messageBuffer.append("total:"+total);
						messageBuffer.append(" ");
						messageBuffer.append("done:"+completed);
						messageBuffer.append(" ");
						messageBuffer.append("error:"+error);
						messageBuffer.append(" ");
						messageBuffer.append("retry:"+retry);
						break;
					}
					message = messageBuffer.toString();
					synchronized(lastMessage) {
						if(!message.equals(lastMessage)) {
							info(message.toString());
							lastMessage = message;
						}
					}
				}
				if(!jobActive.get()) {
					main.interrupt();
				}	
			}
			else {
				debug("OR publication ignored...job not active");
			}
		}
	}
	
	public void setDuccEventDispatcher(DuccEventDispatcher eventDispatcher) {
		throw new RuntimeException();
	}
	
	protected int help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccJobMonitor.class.getName(), options);
		return 1;
	}
	
	public int run(String[] args) throws Exception {
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
				return help(options);
			}
			if(commandLine.getOptions().length == 0) {
				return help(options);
			}
			/*
			 * require DUCC_HOME 
			 */
			String ducc_home_key = "DUCC_HOME";
			String ducc_home = System.getenv(ducc_home_key);
			if(ducc_home == null) {
				duccMessageProcessor.err("missing required environment variable: "+ducc_home_key);
				return 1;
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
				//logger = logger_ts;
			}
			/*
			 * verbosity
			 */
			if (commandLine.hasOption(DuccUiConstants.name_debug)) {
				//logger.setLevel(Level.DEBUG);
				debug = true;
			}
			/*
			 * cancel job enabled
			 */
			if (commandLine.hasOption(DuccUiConstants.name_monitor_cancel_job_on_interrupt)) {
				cancel_job_on_interrupt = true;
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
			/*
			 * broker & endpoint
			 */
			if (commandLine.hasOption(DuccUiConstants.name_service_broker)) {
				broker = commandLine.getOptionValue(DuccUiConstants.name_service_broker);
			}
			if (commandLine.hasOption(DuccUiConstants.name_service_endpoint)) {
				endpoint = commandLine.getOptionValue(DuccUiConstants.name_service_endpoint);
			}
		}
		/* 
		 * echo
		 */
		debug("jmsProvider="+jmsProvider);
		debug("broker="+broker);
		debug("endpoint="+endpoint);
		debug("id="+jobId);
		
		main = Thread.currentThread();
		
		Thread killer = new Killer(main);
		Runtime.getRuntime().addShutdownHook(killer);
		
		start();
		
		StringBuffer msgName = new StringBuffer();
		msgName.append("id:"+jobId);
		msgName.append(" ");
		msgName.append("location:");
		msgName.append(ManagementFactory.getRuntimeMXBean().getName());
		info(msgName.toString());
		
		debug("monitor start");
		while(jobActive.get()) {
			try {
				Thread.sleep(wakeupTime);
			} catch (InterruptedException e) {
				debug(e);
			}
			debug("monitor active...");
		}
		debug("monitor stop");
		stop();
		
		Runtime.getRuntime().removeShutdownHook(killer);
		
		showErrors();
		
		StringBuffer msgRc = new StringBuffer();
		msgRc.append("id:"+jobId);
		msgRc.append(" ");
		msgRc.append("rc:"+rc.get());
		info(msgRc.toString());
		
		return rc.get();
	}
	
	private void dumpFile(String fileName) {
		try {
			String prefix = "id:"+jobId+" ";
			String data = "file:"+fileName;
			info(prefix+data);
			FileInputStream fis = new FileInputStream(fileName);
			DataInputStream dis = new DataInputStream(fis);
			BufferedReader br = new BufferedReader(new InputStreamReader(dis));
			int maxErrors = 1;
			int maxLines = MAXLINES;
			int lines = 0;
			int errors = 0;
			while(true) {
				String line = br.readLine();
				lines ++;
				if(line == null) {
					break;
				}
				String tline = line.trim();
				if(tline.length() == 0) {
					continue;
				}
				String[] tokens = tline.split(" ");
				if(tokens.length > 5) {
					if(tokens[5].trim().equals("ERROR")) {
						errors++;
					}
				}
				info(prefix+line);
				if(lines > maxLines) {
					info(prefix+"more...");
					break;
				}
				if(errors > maxErrors) {
					info(prefix+"more...");
					break;
				}
			}
			br.close();
			dis.close();
			fis.close();
		}
		catch(Exception e) {
			debug(e);
		}
	}
	
	private void showErrors() {
		if(job != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(job.getLogDirectory());
			if(!job.getLogDirectory().endsWith(File.separator)) {
				sb.append(File.separator);
			}
			sb.append(job.getDuccId().getFriendly()+File.separator);
			sb.append("jd.err.log");
			try {
				String fileName = sb.toString();
				File file = new File(fileName);
				if(file.canRead()) {
					dumpFile(fileName);
				}
			}
			catch(Exception e) {
				debug(e);
			}
		}
	}
	
	private class Killer extends Thread {
		
		public Killer(Thread thread) {
		}
		
		public void run() {
			StringBuffer message = new StringBuffer();
			if(cancel_job_on_interrupt) {
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
       		arrayList.add("--"+DuccUiConstants.name_service_broker);
       		arrayList.add(broker);
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
			duccJobMonitor.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}

}
