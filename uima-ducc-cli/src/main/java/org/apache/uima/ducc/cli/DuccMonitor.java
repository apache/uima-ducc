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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;

import com.google.gson.Gson;

public abstract class DuccMonitor  {

	protected static final int RC_SUCCESS = 0;
	protected static final int RC_FAILURE = 1;
	protected static final int RC_HELP = RC_FAILURE;
	
	protected static final String NotFound = "NotFound";
	protected static final String StateRunning = "Running";
	protected static final String StateCompleting = "Completing";
	protected static final String StateCompleted = "Completed";
	
	private Options options = new Options();
	
	private UiOption[] opts = new UiOption[0];
    
	private String id = null;
	
	private AtomicBoolean flag_cancel_on_interrupt = new AtomicBoolean(false);
	private AtomicBoolean flag_debug = new AtomicBoolean(false);
	private AtomicBoolean flag_error = new AtomicBoolean(true);
	private AtomicBoolean flag_info = new AtomicBoolean(true);
	private AtomicBoolean flag_trace = new AtomicBoolean(false);
	private AtomicBoolean flag_timestamp = new AtomicBoolean(false);
	
	private AtomicBoolean flag_observer = new AtomicBoolean(true);
	
	private int milliseconds 	= 1;
	private int seconds			= 1000*milliseconds;
	private int wakeupInterval 	= 15*seconds;
	private int urlTimeout 		= 1*60*seconds;
	
	private Thread main = null;
	private DuccPropertiesResolver duccPropertiesResolver = null;
	
	private DuccContext context = null;
    IDuccCallback messageProcessor = null;
	
	private SynchronizedSimpleDateFormat sdf = new SynchronizedSimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	
	private UiOption[] optsSubmitJob = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Quiet, 
        UiOption.Timestamp, 
        UiOption.JobId,
        UiOption.CancelOnInterrupt,
        UiOption.CancelJobOnInterrupt,
    };
    
	private UiOption[] optsMonitorJob = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Quiet, 
        UiOption.Timestamp, 
        UiOption.JobId,
    };
	
	private UiOption[] optsSubmitManagedReservation = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Quiet, 
        UiOption.Timestamp, 
        UiOption.ManagedReservationId,
        UiOption.CancelOnInterrupt,
        UiOption.CancelManagedReservationOnInterrupt,
    };
	
	private UiOption[] optsMonitorManagedReservation = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Quiet, 
        UiOption.Timestamp, 
        UiOption.ManagedReservationId,
    };
	
	protected DuccMonitor(DuccContext context, boolean submit) {
		initialize(context, submit, new DefaultCallback());
	}
	
	protected DuccMonitor(DuccContext context, boolean submit, IDuccCallback messageProcessor) {
		initialize(context, submit, messageProcessor);
	}
	
	public abstract void help(Options options);
	public abstract void cancel();
	public abstract String getUrl(String id);
	
	public String getHost() {
		String host = duccPropertiesResolver.getFileProperty("ducc.ws.node");
		if(host == null) {
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
    
    private void initialize(DuccContext context, boolean submit, IDuccCallback messageProcessor) {
        // context
        this.context = context;
        // submit
        if(context != null) {
            switch(context) {
            case Job:
                if(submit) {
                    opts = optsSubmitJob;
                }
                else {
                    opts = optsMonitorJob;
                }   
                break;
            case ManagedReservation:
                if(submit) {
                    opts = optsSubmitManagedReservation;
                }
                else {
                    opts = optsMonitorManagedReservation;
                }   
                break;
            default:
                break;
            }
        }
        options = makeOptions(opts,false);
        // message processor
        if(messageProcessor != null) {
            this.messageProcessor = messageProcessor;
        }
    }
    
    protected Options makeOptions(UiOption[] optlist, boolean strict) {
        Options opts = new Options();
        for ( UiOption opt : optlist ) {
            OptionBuilder.withDescription(opt.makeDesc());
            OptionBuilder.withLongOpt   (opt.pname()); 
            if ( opt.argname() == null ) { 
                OptionBuilder.hasArg(false);
            }
            else {
                OptionBuilder.withArgName(opt.argname());
                if ( opt.multiargs() ) {
                    OptionBuilder.hasArgs();
                }
                else {
                    OptionBuilder.hasArgs(1);
                }
            }
            if ( strict && opt.required() ) {
                OptionBuilder.isRequired();
            }
            Option o = OptionBuilder.create();
            opts.addOption(o);
        }
        return opts;
    }
    
    protected void trace(String message) {
        if(flag_trace.get()) {
            messageProcessor.status(timestamp(message));
        }
    }
    
    protected void debug(String message) {
        if(flag_debug.get()) {
            messageProcessor.status(timestamp(message));
        }
    }
    
    protected void debug(Exception e) {
        if(flag_debug.get()) {
            messageProcessor.status(e.toString());
        }
    }
    
    private void info(String message) {
        if(flag_info.get()) {
            messageProcessor.status(timestamp(message));
        }
    }
    
    private void error(String message) {
        if(flag_error.get()) {
            messageProcessor.status(timestamp(message));
        }
    }
    
    protected String timestamp(String message) {
        String tMessage = message;
        if(flag_timestamp.get()) {
            String date = sdf.format(new java.util.Date());
            tMessage = date+" "+message;
        }
        return tMessage;
    }
    
    private String details(MonitorInfo monitorInfo) {
        StringBuffer sb = new StringBuffer();
        switch(context) {
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
        String rate = duccPropertiesResolver.getFileProperty("ducc.orchestrator.state.publish.rate");
        try {
            wakeupInterval = Integer.parseInt(rate);
        }
        catch(Exception e) {
            debug(e);
        }
    }
    
    private int runInternal(String[] args) throws Exception {
        // DUCC_HOME
        String ducc_home = Utils.findDuccHome();
        if(ducc_home == null) {
            messageProcessor.status("Missing required environment variable: DUCC_HOME");
            return RC_FAILURE;
        }
        // Ingest ducc.properties
        duccPropertiesResolver = DuccPropertiesResolver.getInstance();
        // Parse
        synchronized(DuccMonitor.class) {
            CommandLineParser parser = new PosixParser();
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption(DuccUiConstants.name_help)) {
                help(options);
                return RC_HELP;
            }
            if(commandLine.getOptions().length == 0) {
                help(options);
                return RC_HELP;
            }
            if (commandLine.hasOption(UiOption.Timestamp.pname())) {
                flag_timestamp.set(true);
            }
            if (commandLine.hasOption(UiOption.Quiet.pname())) {
                flag_info.set(false);
                flag_error.set(false);
            }
            if (commandLine.hasOption(UiOption.Debug.pname())) {
                flag_debug.set(true);
            }
            if (commandLine.hasOption(UiOption.CancelOnInterrupt.pname())) {
                flag_cancel_on_interrupt.set(true);
            }
            if (commandLine.hasOption(UiOption.CancelJobOnInterrupt.pname())) {
                flag_cancel_on_interrupt.set(true);
            }
            if (commandLine.hasOption(UiOption.CancelManagedReservationOnInterrupt.pname())) {
                flag_cancel_on_interrupt.set(true);
            }
            if (commandLine.hasOption(UiOption.JobId.pname())) {
                id = commandLine.getOptionValue(UiOption.JobId.pname());
            }
            else if (commandLine.hasOption(UiOption.ManagedReservationId.pname())) {
                id = commandLine.getOptionValue(UiOption.ManagedReservationId.pname());
            }
            else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(110);
                formatter.printHelp(DuccJobMonitor.class.getName(), options);
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
        StringBuffer message = new StringBuffer();
        message.append("id:"+id);
        message.append(" ");
        message.append("location:");
        message.append(ManagementFactory.getRuntimeMXBean().getName());
        info(message.toString());
        // Poll until finished
        while(flag_observer.get()) {
            String json = getSingleLineStatus(urlString);
            if(json != null) {
                debug(json);
                Gson gson = new Gson();
                MonitorInfo monitorInfo = gson.fromJson(json, MonitorInfo.class);
                int stateCount = monitorInfo.stateSequence.size();
                debug("states:"+stateCount);
                if(stateCount <= 0) {
                    message = new StringBuffer();
                    message.append("id:"+id);
                    message.append(" ");
                    message.append("state:"+NotFound);
                    thisMessage = message.toString();
                    info(thisMessage);
                    message = new StringBuffer();
                    message.append("id:"+id);
                    message.append(" ");
                    message.append("rc:"+RC_FAILURE);
                    thisMessage = message.toString();
                    info(thisMessage);
                    return RC_FAILURE;
                }
                String state = "";
                Iterator<String> states = monitorInfo.stateSequence.iterator();
                while(states.hasNext()) {
                    state = states.next();
                    debug("list:"+state);
                }
                message = new StringBuffer();
                message.append("id:"+id);
                message.append(" ");
                message.append("state:"+state);
                if(state.equals(StateRunning)) {
                    message.append(details(monitorInfo));
                }
                else if(state.equals(StateCompleting)) {
                    flag_cancel_on_interrupt.set(false);
                    message.append(details(monitorInfo));
                }
                else if(state.equals(StateCompleted)) {
                    flag_cancel_on_interrupt.set(false);
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
                            if(monitorInfo.code.equals("0")) {
                                message = new StringBuffer();
                                message.append("id:"+id);
                                message.append(" ");
                                message.append("code:"+monitorInfo.code);
                                thisMessage = message.toString();
                                info(thisMessage);
                                message = new StringBuffer();
                                message.append("id:"+id);
                                message.append(" ");
                                message.append("rc:"+RC_SUCCESS);
                                thisMessage = message.toString();
                                info(thisMessage);
                                return RC_SUCCESS;
                            }
                            else {
                                message = new StringBuffer();
                                message.append("id:"+id);
                                message.append(" ");
                                message.append("code:"+monitorInfo.code);
                                thisMessage = message.toString();
                                info(thisMessage);
                                message = new StringBuffer();
                                message.append("id:"+id);
                                message.append(" ");
                                message.append("rc:"+RC_FAILURE);
                                thisMessage = message.toString();
                                info(thisMessage);
                                return RC_FAILURE;
                            }
                        }
                        else {
                            if(!monitorInfo.errorLogs.isEmpty()) {
                                message = new StringBuffer();
                                message.append("id:"+id);
                                message.append(" ");
                                ArrayList<String> errorLogs = monitorInfo.errorLogs;
                                for(String errorLog : errorLogs) {
                                    message.append("file:"+errorLog);
                                }
                                thisMessage = message.toString();
                                info(thisMessage);
                            }
                            message = new StringBuffer();
                            message.append("id:"+id);
                            message.append(" ");
                            message.append("rc:"+RC_FAILURE);
                            thisMessage = message.toString();
                            info(thisMessage);
                            return RC_FAILURE;
                        }
                    }
                }
            }
            else {
                error("error: accessing "+urlString);
            }
            long start = System.currentTimeMillis();
            long end = start;
            while(!isTimeExpired(start,end,wakeupInterval)) {
                if(!flag_observer.get()) {
                    break;
                }
                try {
                    Thread.sleep(wakeupInterval);
                } 
                catch (InterruptedException e) {
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
        if(diff >= interval) {
            retVal = true;
        }
        trace("start:"+start+" "+"end:"+end+" "+"diff:"+diff+" "+"interval:"+interval+" "+"result:"+retVal);
        return retVal;
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
    
    private class Killer extends Thread {
        
        public Killer(Thread thread) {
        }
        
        public void run() {
            StringBuffer message = new StringBuffer();
            if(flag_cancel_on_interrupt.get()) {
                message.append("killer: cancel");
                cancel();
            }
            else {
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
        } 
        catch (Exception e) {
            messageProcessor.status(e.toString());
        }
        debug("rc="+code);
        return code;
    }

}
