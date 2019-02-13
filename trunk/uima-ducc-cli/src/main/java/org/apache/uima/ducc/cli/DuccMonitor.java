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

import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcherCl;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
// import org.apache.commons.cli.CommandLine;
// import org.apache.commons.cli.CommandLineParser;
// import org.apache.commons.cli.HelpFormatter;
// import org.apache.commons.cli.Options;
// import org.apache.commons.cli.PosixParser;

public abstract class DuccMonitor {

	protected static final int RC_SUCCESS = 0;
	protected static final int RC_FAILURE = 1;
	protected static final int RC_HELP = RC_FAILURE;

	protected static final String NotFound = "NotFound";
	protected static final String StateRunning = "Running";
	protected static final String StateCompleting = "Completing";
	protected static final String StateCompleted = "Completed";
	protected static final String StateWaitingForResources = "WaitingForResources";
	protected static final String StateAssigned = "Assigned";

    protected CommandLine command_line = null;

	// private Options options = new Options();

	private IUiOption[] opts = new UiOption[0];

	private String id = null;

	private AtomicBoolean flag_cancel_on_interrupt = new AtomicBoolean(false);
	private AtomicBoolean flag_debug = new AtomicBoolean(false);
	private AtomicBoolean flag_error = new AtomicBoolean(true);
	private AtomicBoolean flag_info = new AtomicBoolean(true);
	private AtomicBoolean flag_trace = new AtomicBoolean(false);
	private AtomicBoolean flag_timestamp = new AtomicBoolean(false);

	private AtomicBoolean flag_observer = new AtomicBoolean(true);

	private int milliseconds = 1;
	private int seconds = 1000 * milliseconds;
	private int wakeupInterval = 15 * seconds;
	private int urlTimeout = 1 * 60 * seconds;

	private Thread main = null;
	private DuccPropertiesResolver duccPropertiesResolver = null;

	private DuccContext context = null;
	protected IDuccCallback messageProcessor = null;

	private volatile MonitorInfo previousMonitorInfo = null;
	
	private String delayedRunning = null;
	
	private SynchronizedSimpleDateFormat sdf = new SynchronizedSimpleDateFormat(
			"dd/MM/yyyy HH:mm:ss");

	private IUiOption[] optsSubmitJob = new UiOption[] { UiOption.Help,
			UiOption.Debug, UiOption.Quiet, UiOption.Timestamp, UiOption.JobId,
			UiOption.CancelOnInterrupt, };

	private IUiOption[] optsMonitorJob = new UiOption[] { UiOption.Help,
			UiOption.Debug, UiOption.Quiet, UiOption.Timestamp, UiOption.JobId, };

	private IUiOption[] optsSubmitReservation = new UiOption[] {
			UiOption.Help, UiOption.Debug, UiOption.Quiet, UiOption.Timestamp,
			UiOption.ReservationId, UiOption.CancelOnInterrupt, };

	private IUiOption[] optsMonitorReservation = new UiOption[] {
			UiOption.Help, UiOption.Debug, UiOption.Quiet, UiOption.Timestamp,
			UiOption.ReservationId, };
	
	private IUiOption[] optsSubmitManagedReservation = new UiOption[] {
			UiOption.Help, UiOption.Debug, UiOption.Quiet, UiOption.Timestamp,
			UiOption.ManagedReservationId, UiOption.CancelOnInterrupt, };

	private IUiOption[] optsMonitorManagedReservation = new UiOption[] {
			UiOption.Help, UiOption.Debug, UiOption.Quiet, UiOption.Timestamp,
			UiOption.ManagedReservationId, };

	protected DuccMonitor(DuccContext context, boolean submit) {
		initialize(context, submit, new DefaultCallback());
	}

	protected DuccMonitor(DuccContext context, boolean submit,
			IDuccCallback messageProcessor) {
		initialize(context, submit, messageProcessor);
	}

	public void help(IUiOption[] options)
    {
        System.out.println(command_line.formatHelp(this.getClass().getName()));
    }

	public abstract void cancel();

	public abstract String getUrl(String id);

	public String getHost() {
		String host = duccPropertiesResolver.getFileProperty("ducc.ws.node");
		if (host == null) {
			host = duccPropertiesResolver.getFileProperty("ducc.head");
		}
		return host;
	}

	public String getPort() {
		String port = duccPropertiesResolver.getFileProperty("ducc.ws.port");
		return port;
	}

	public String getId() {
		return id;
	}

	private void initialize(DuccContext context, boolean submit,
			IDuccCallback messageProcessor) {
		// context
		this.context = context;
		// submit
		if (context != null) {
			switch (context) {
			case Job:
				if (submit) {
					opts = optsSubmitJob;
				} else {
					opts = optsMonitorJob;
				}
				break;
			case Reservation:
				if (submit) {
					opts = optsSubmitReservation;
				} else {
					opts = optsMonitorReservation;
				}
				break;
			case ManagedReservation:
				if (submit) {
					opts = optsSubmitManagedReservation;
				} else {
					opts = optsMonitorManagedReservation;
				}
				break;
			default:
				break;
			}
		}
		// options = CliBase.makeOptions(opts);
		// message processor
		if (messageProcessor != null) {
			this.messageProcessor = messageProcessor;
		}
	}

	protected void trace(String message) {
		if (flag_trace.get()) {
			messageProcessor.status(timestamp(message));
		}
	}

	protected void debug(String message) {
		if (flag_debug.get()) {
			messageProcessor.status(timestamp(message));
		}
	}

	protected void debug(Exception e) {
		if (flag_debug.get()) {
			messageProcessor.status(e.toString());
		}
	}

	private void info(String message) {
		if (flag_info.get()) {
			messageProcessor.status(timestamp(message));
		}
	}

	@SuppressWarnings("unused")
	private void error(String message) {
		if (flag_error.get()) {
			messageProcessor.status(timestamp(message));
		}
	}

	protected String timestamp(String message) {
		String tMessage = message;
		if (flag_timestamp.get()) {
			String date = sdf.format(new java.util.Date());
			tMessage = date + " " + message;
		}
		return tMessage;
	}

	private String details(MonitorInfo monitorInfo) {
		StringBuffer sb = new StringBuffer();
		switch (context) {
		case Job:
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
			break;
		}
		return sb.toString();
	}

	private void adjustWakeupInterval() {
		String rate = duccPropertiesResolver
				.getFileProperty("ducc.orchestrator.state.publish.rate");
		try {
			wakeupInterval = Integer.parseInt(rate);
		} catch (Exception e) {
			debug(e);
		}
	}

	private ArrayList<String> seenRemotePids = new ArrayList<String>();
	
	private void displayRemotePids(MonitorInfo monitorInfo) {
		if(monitorInfo != null) {
			if(monitorInfo.remotePids != null) {
				for(String remotePid : monitorInfo.remotePids) {
					if(!seenRemotePids.contains(remotePid)) {
						seenRemotePids.add(remotePid);
						switch(context) {
						case Job:
							break;
						default:
							StringBuffer message = new StringBuffer();
							message.append("id:" + id);
							message.append(" remote:" + remotePid);
							info(message.toString());
							break;
						}
					}
				}
			}
		}
	}
	
	private int runInternal(String[] args) throws Exception {
		// DUCC_HOME
		String ducc_home = Utils.findDuccHome();
		if (ducc_home == null) {
			messageProcessor
					.status("Missing required environment variable: DUCC_HOME");
			return RC_FAILURE;
		}
		// Ingest ducc.properties
		duccPropertiesResolver = DuccPropertiesResolver.getInstance();
		// Parse
		synchronized (DuccMonitor.class) {
			command_line = new CommandLine(args, opts);
            try {
                command_line.parse();
            } catch ( IllegalArgumentException e ) {
                System.out.println("Illegal arguments: " + e.getMessage());
                help(opts);
                return RC_HELP;
            }

			if (command_line.contains(UiOption.Help)) {
				help(opts);
				return RC_HELP;
			}
            
			if (command_line.contains(UiOption.Timestamp)) {
				flag_timestamp.set(true);
			}
			if (command_line.contains(UiOption.Quiet)) {
				flag_info.set(false);
				flag_error.set(false);
			}
			if (command_line.contains(UiOption.Debug)) {
				flag_debug.set(true);
			}
			if (command_line.contains(UiOption.CancelOnInterrupt)) {
				flag_cancel_on_interrupt.set(true);
			}
			if (command_line.contains(UiOption.JobId)) {
				id = command_line.get(UiOption.JobId);
			}
			else if (command_line.contains(UiOption.ManagedReservationId)) {
				id = command_line.get(UiOption.ManagedReservationId);
            } 
			else if (command_line.contains(UiOption.ReservationId)) {
				id = command_line.get(UiOption.ReservationId);
            } 
			else {
                System.out.println(command_line.formatHelp(DuccJobMonitor.class.getName()));
				return RC_HELP;
			}
		}
		// Handle Ctl-C
		main = Thread.currentThread();
		Thread killer = new Killer(main);
		Runtime.getRuntime().addShutdownHook(killer);
		// Setup polling
		adjustWakeupInterval();
		String urlString = getUrl(id);
		String lastMessage = "";
		String thisMessage = "";
		String lastRationale = "";
		String thisRationale = "";
		StringBuffer message = new StringBuffer();
		message.append("id:" + id);
		message.append(" location:");
		message.append(ManagementFactory.getRuntimeMXBean().getName());
		info(message.toString());
		debug(urlString);
		// Poll until finished - retry if the WS appears to be down
		boolean connectionFailed = false;
		while (flag_observer.get()) {
			DuccEventHttpDispatcherCl dispatcher = null;
			MonitorInfo monitorInfo = null;
			try {
				dispatcher = new DuccEventHttpDispatcherCl(urlString, urlTimeout);
				monitorInfo = (MonitorInfo) dispatcher.dispatchJson(MonitorInfo.class);
				if (connectionFailed) {
					info("id:" + id + " warning:Connection to DUCC restored");
					connectionFailed = false;
				}
			} catch (ConnectException e) {
				if (!connectionFailed) {
					info("id:" + id + " warning:Connection to DUCC failed -- retrying");
					connectionFailed = true;
				}
			}

            if ( monitorInfo != null ) {
            	// It is  possible after OR "warm" start that
            	// work item processing status information 
            	// may be missing or incorrect for a short time.
            	// Therefore, we assure that newly arrived
            	// information is not a regression from the
            	// last good one received, if any.
            	if(monitorInfo.isRegression(previousMonitorInfo)) {
            		continue;
            	}
            	previousMonitorInfo = monitorInfo;
            	displayRemotePids(monitorInfo);
				int stateCount = monitorInfo.stateSequence.size();
				debug("states:" + stateCount);
				// If OR or network is very slow WS may not have seen the job yet so just report NotFound
				// No longer give up and possibly falsely cancel the job
				String state = NotFound;
				Iterator<String> states = monitorInfo.stateSequence.iterator();
				while (states.hasNext()) {
					state = states.next();
					debug("list:" + state);
				}
				message = new StringBuffer();
				message.append("id:" + id);
				message.append(" state:" + state);
				if (state.equals(StateRunning)) {
					message.append(details(monitorInfo));
				} else if (state.equals(StateCompleting)) {
					flag_cancel_on_interrupt.set(false);
					message.append(details(monitorInfo));
				} else if (state.equals(StateCompleted)) {
					flag_cancel_on_interrupt.set(false);
					message.append(details(monitorInfo));
				}
				else if (context == DuccContext.Reservation && state.equals(StateAssigned)) {       // A reservation has completed
					flag_cancel_on_interrupt.set(false);
					message.append(details(monitorInfo));
				}
				thisMessage = message.toString();
				if (!thisMessage.equals(lastMessage)) {
					boolean suppress = false;
					if(state.equals(StateRunning)) {
						if(seenRemotePids.size() == 0) {
							suppress = true;
							if(delayedRunning == null) {
								delayedRunning = message.toString();
							}
						}
						else {
							delayedRunning = null;
						}
					}
					if(!suppress) {
						if(delayedRunning != null) {
							info(delayedRunning);
							delayedRunning = null;
						}
						info(thisMessage);
						lastMessage = thisMessage;
					}
				}
				if (state.equals(StateWaitingForResources)) {
					if (!monitorInfo.rationale.equals("")) {
						thisRationale = monitorInfo.rationale;
						if (!thisRationale.equals(lastRationale)) {
							info(thisRationale);
							lastRationale = thisRationale;
						}
					}
				}
				if (context == DuccContext.Reservation && state.equals(StateAssigned)) {
					if(monitorInfo.nodes != null) {
						if(monitorInfo.nodes.size() > 0) {
							StringBuffer sb = new StringBuffer();
							sb.append("nodes: ");
							for(String node : monitorInfo.nodes) {
								sb.append(node);
								sb.append(" ");
							}
							String nodes = sb.toString().trim();
							info(nodes);
						}
					}
					return RC_SUCCESS;
				}
				if (state.equals(StateCompleted)) {
					// See Jira 2911
					//if (monitorInfo.procs.equals("0")) {
						if (monitorInfo.total.equals(monitorInfo.done)) {
							if (!monitorInfo.rationale.equals("")) {
								message = new StringBuffer();
								message.append("id:" + id);
								message.append(" rationale:" + monitorInfo.rationale);
								thisMessage = message.toString();
								info(thisMessage);
							}
							int rc = RC_FAILURE;
							message = new StringBuffer();
							message.append("id:" + id);
							try {
								rc = Integer.parseInt(monitorInfo.code);
								message.append(" rc:" + rc);
							} catch (NumberFormatException e) {
								message.append(" code:" + monitorInfo.code);
							}
							thisMessage = message.toString();
							info(thisMessage);
							return rc;
						} else {
							if (!monitorInfo.errorLogs.isEmpty()) {
								message = new StringBuffer();
								message.append("id:" + id);
								List<String> errorLogs = monitorInfo.errorLogs;
								for (String errorLog : errorLogs) {
									message.append(" file:" + errorLog);
								}
								thisMessage = message.toString();
								info(thisMessage);
							}
							if (!monitorInfo.rationale.equals("")) {
								message = new StringBuffer();
								message.append("id:" + id);
								message.append(" rationale:" + monitorInfo.rationale);
								thisMessage = message.toString();
								info(thisMessage);
							}
							message = new StringBuffer();
							message.append("id:" + id);
							message.append(" rc:" + RC_FAILURE);
							thisMessage = message.toString();
							info(thisMessage);
							return RC_FAILURE;
						}
					//}
				}
			}
			long start = System.currentTimeMillis();
			long end = start;
			while (!isTimeExpired(start, end, wakeupInterval)) {
				if (!flag_observer.get()) {
					break;
				}
				try {
					Thread.sleep(wakeupInterval);
				} catch (InterruptedException e) {
					debug(e);
				}
				end = System.currentTimeMillis();
			}

		}
		return RC_SUCCESS;
	}

	private boolean isTimeExpired(long start, long end, long interval) {
		boolean retVal = false;
		long diff = end - start;
		if (diff >= interval) {
			retVal = true;
		}
		trace("start:" + start + " " + "end:" + end + " " + "diff:" + diff
				+ " " + "interval:" + interval + " " + "result:" + retVal);
		return retVal;
	}

//	private String getSingleLineStatus(String urlString) {
//		String line = null;
//		URL url = null;
//		try {
//			url = new URL(urlString);
//			URLConnection uc = url.openConnection();
//			uc.setReadTimeout(urlTimeout);
//			BufferedReader br = new BufferedReader(new InputStreamReader(
//					uc.getInputStream()));
//			line = br.readLine();
//			br.close();
//		} catch (MalformedURLException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return line;
//	}

	private class Killer extends Thread {

		public Killer(Thread thread) {
		}

		public void run() {
			StringBuffer message = new StringBuffer();
			if (flag_cancel_on_interrupt.get()) {
				message.append("killer: cancel");
				cancel();
			} else {
				message.append("killer: no cancel");
			}
			debug(message.toString());
			flag_observer.set(false);
		}
	}

	public int run(String[] args) {
		int code = RC_FAILURE;
		try {
			code = runInternal(args);
		} catch (Exception e) {
			messageProcessor.status("ERROR: " + e.toString());
			e.printStackTrace();
		}
		debug("rc=" + code);
		return code;
	}

}
