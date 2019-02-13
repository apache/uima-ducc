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
package org.apache.uima.ducc.orchestrator.system.events.log;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.uima.ducc.common.utils.LoggingException;
import org.apache.uima.ducc.common.utils.id.DuccId;


//
// Note: there are some System.out.printlns here for debugging purposes.  These things will only
// be invoked during boot of the ducc component and are invaluable for debug when we use
// ducc.py so they are left here intentionally.
//
public class DuccLogger
{
    private Logger logger;
    private String component = "";

    private static DuccLoggingThread log_thread = null;
    private static LinkedBlockingQueue<DuccLoggingEvent> events = null;
    private static AtomicBoolean threaded = new AtomicBoolean(false);
    private static boolean watchdogStarted = false;

    private final static String DEFAULT_COMPONENT = "DUCC";
    private static List<Logger> nonDuccLoggers = new ArrayList<Logger>();

    private boolean debug = System.getProperty("log4j.debug") != null;    // Use the log4j debugging flag

    static synchronized protected void initLogger()
    {
        if ( log_thread == null ) {
            events = new LinkedBlockingQueue<DuccLoggingEvent>();
            log_thread = new DuccLoggingThread();
            log_thread.setName("DuccLoggerThread");
            log_thread.setDaemon(true);
            log_thread.start();
        }
    }

    static public DuccLogger getLogger(@SuppressWarnings("rawtypes") Class claz, String component)
    {
        return new DuccLogger(claz, component);
    }

    static public DuccLogger getLogger(String claz, String component)
    {
        return new DuccLogger(claz, component);
    }

    // Usually just called by DuccService, with the global component logger as base
    // This constructs a logger for the given class, and then add all the appenders
    // from 'this'. Be careful configuring log4j.xml, you probably don't want any
    // appenders on the class 'claz' or you'll get unexpected extra log files.
    public DuccLogger getLoggerFor(String claz)
    {
        if ( logger == null ) {
            System.out.println("DuccLogger is not initialized, cannot create logger for(" + claz + ").");
            return this;
        }

        if ( claz == null ) {
            throw new IllegalArgumentException("New log name must not be null");
        }

        DuccLogger ret = getLogger(claz, this.getComponent());

        Category l = logger;
        // List<Appender> appenders= new ArrayList<Appender>();
        while ( l != null ) {
        	@SuppressWarnings("rawtypes")
			Enumeration apps = l.getAllAppenders();                        
            if ( apps.hasMoreElements() ) {                
                while (apps.hasMoreElements() ) {
                    Appender app = (Appender) apps.nextElement();
                    if ( ret.getAppender(app.getName()) == null ) {
                        ret.addAppender(app);
                    }
                }
            } 
            l = l.getParent();
        }
        return ret;
    }

    // PACKAGE protection
    void removeAllAppenders()
    {
        this.logger.removeAllAppenders();
    }

    // PACKAGE protection
    void addAppender(Appender app)
    {
        this.logger.addAppender(app);
    }

    Appender getAppender(String name)
    {
        return this.logger.getAppender(name);
    }

    static public void setUnthreaded()
    {
        //threaded.set(true);
        System.out.println("setUnthreaded is not supported.");
    }

    static public void setThreaded()
    {
    	threaded.set(true);
    }

    public DuccLogger(String claz, String component)
    {
        // initLogger();

        // UIMA-4186, use log4j API for configuration
        String ducc_home = System.getProperty("DUCC_HOME");
        if ( ducc_home == null ) { 
            System.out.println("WARNING: Cannot find system property DUCC_HOME to configure ducc logger.  Using default log4j configurator.");
        } else {
            if ( ! watchdogStarted ) {
            	// Explicitly set the config file since the search for the default uses 
            	// ClassLoader.getSystemResource() so may find one in the user's classpath
                // UIMA-5183 First check for a node-specific configuration file
                String configFile = System.getProperty("DUCC_HOME") + "/resources/log4j.xml";
                String nodeName = System.getProperty("DUCC_NODENAME");
                if (nodeName != null && !nodeName.isEmpty()) {
                    nodeName = nodeName.split("\\.",2)[0];  // Drop domain suffiz
                    File cfgFile = new File(System.getProperty("DUCC_HOME") + "/resources/"+nodeName+"-log4j.xml");
                    if (cfgFile.exists()) {
                        configFile = cfgFile.getAbsolutePath();
                        System.out.println("DuccLogger will use configuration file: " + configFile);
                    }
                }
            	String usersValue = System.setProperty("log4j.configuration", "file:" + configFile);
                DOMConfigurator.configureAndWatch(configFile);
                if (usersValue == null) {
                	System.clearProperty("log4j.configuration");
                } else {
                	System.setProperty("log4j.configuration", usersValue);
                }
                watchdogStarted = true;
            } 
        }

        //
        // Try to set component from calling thread if not set.  
        //
        // If all else fails, set it to "DUCC"
        //
    	if ( debug) System.out.println("Creating logger '" + claz + "' with component " + component);
        if ( component == null ) {
            component = (String) MDC.get("COMPONENT");
            if ( component == null ) {
                component = DEFAULT_COMPONENT;
            }
            @SuppressWarnings("rawtypes")
			Enumeration all_loggers = LogManager.getCurrentLoggers();
            while (all_loggers.hasMoreElements() ) {
                Logger l = (Logger) all_loggers.nextElement();
                String n = l.getName();
                if ( debug ) System.out.println(" ===> Configured loggers " + n);
                if ( ! n.startsWith("org.apache.uima.ducc" ) ) {
                    if ( debug ) System.out.println("      Special logger: " + n);
                    nonDuccLoggers.add(l);
                }
            }
        }

    	this.component = component;
        this.logger = Logger.getLogger(claz);
        MDC.put("COMPONENT", component);

        ErrorHandler errHandler = new DuccLogErrorHandler(this);
        @SuppressWarnings("rawtypes")
		Enumeration appenders = logger.getAllAppenders();
        while (appenders.hasMoreElements() ) {
            Appender app = (Appender) appenders.nextElement();
            app.setErrorHandler(errHandler);
        }
    }

    public DuccLogger(@SuppressWarnings("rawtypes") Class claz, String component)
    {
        this(claz.getName(), component);
    }
        
    public DuccLogger(@SuppressWarnings("rawtypes") Class claz)
    {
        this(claz, null);
    }
    
    public DuccLogger(String claz)
    {
        this(claz, null);
    }

    public boolean isDefaultLogger()
    {
        return this.component.equals(DEFAULT_COMPONENT);
    }

    public void setAdditionalAppenders()
    {
    	if ( debug ) System.out.println("============ Looking for appenders -----------");
        if ( isDefaultLogger() ) {
            if ( debug ) System.out.println(" ---> Skipping appender search for default component");
            return;
        }

        Category l = logger;
        // List<Appender> appenders= new ArrayList<Appender>();
        while ( l != null ) {
        	@SuppressWarnings("rawtypes")
			Enumeration apps = l.getAllAppenders();                        
            if ( apps.hasMoreElements() ) {
                
                while (apps.hasMoreElements() ) {
                    Appender app = (Appender) apps.nextElement();
                    // appenders.add(app);
                    if ( l.getName().startsWith("org.apache.uima.ducc") ) {
                        if ( debug ) System.out.println(" ---> Found appender " + app.getName() + " on logger " + l.getName());
                        for ( Logger ll : nonDuccLoggers ) {     // put the appender on the non-Ducc logger
                            if ( debug ) System.out.println(" ---> Add appender " + app.getName() + " to logger " + ll.getName());
                            if ( ll.getAppender(app.getName() ) == null ) {
                                ll.addAppender(app);
                            }
                        }
                    } else {
                        if ( debug ) System.out.println(" ---> Skipping non-DUCC appender " + app.getName() + " on logger " + l.getName());
                    }
                }
            } else {
                if ( debug ) System.out.println(" ---> No appenders on logger " + l.getName());
            }
            l = l.getParent();
        }

    }

    public String getComponent() {
    	return component;
    }
    
    public void setLevel(Level l)
    {
        this.logger.setLevel(l);
    }

    public Level getLevel()
    {
        return logger.getLevel();
    }

    public boolean isLevelEnabled(Level l)
    {
        return l.isGreaterOrEqual(logger.getEffectiveLevel());
    }

    public boolean isFatal() 
    {
        return isLevelEnabled(Level.FATAL);
    }

    public boolean isDebug() 
    {
        return isLevelEnabled(Level.DEBUG);
    }

    public boolean isError() 
    {
        return isLevelEnabled(Level.ERROR);
    }

    public boolean isInfo() 
    {
        return isLevelEnabled(Level.INFO);
    }

    public boolean isWarn() 
    {
        return isLevelEnabled(Level.WARN);
    }

    public boolean isTrace() 
    {
        return isLevelEnabled(Level.TRACE);
    }

    protected String formatMsg(DuccId pid, Object ... args)
    {
    	String header = format(pid);
        String msg = formatMsg(args);
        return header + " " + msg;
    }
    
    private void appendStackTrace(StringBuffer s, Throwable t)
    {
    	s.append("\nAt:\n");
        StackTraceElement[] stacktrace = t.getStackTrace();
        for ( StackTraceElement ste : stacktrace ) {
            s.append("\t");
            s.append(ste.toString());
            s.append("\n");
        }
    }

    protected String formatMsg(Object ... args)
    {
    	StringBuffer s = new StringBuffer();
        for ( Object a : args ) {
            if ( a == null ) a = "<null>"; // avoid null pointers

            s.append(" ");
            if ( a instanceof Throwable ) {
            	Throwable t = (Throwable ) a;
                s.append(t.toString());
                s.append("\n");
                appendStackTrace(s, t);
            } else {                
                s.append(a.toString());
            }
        }
        return s.toString();
    }

    public void setDefaultDuccId(String defaultDuccId) {
    	if(defaultDuccId != null) {
    		defaultId = defaultDuccId;
    	}
    }
    
    private String defaultId = "N/A";
    private final String padding = "                ";  // Max padding needed is 19- 3
    
    // UIMA-5190 Pad the N/A value to match the job #s so headings align correctly
    private String format(DuccId duccId) {
    	String id;
        if ( duccId == null ) {
            id = defaultId;
        } else {
            id = duccId.toString();
            int increase = id.length() - defaultId.length();
            if (increase > 0) {
                defaultId += padding.substring(0, increase);
            }
        }
        return id;
    }
    
    protected void setMDC()
    {
        //MDC.put("COMPONENT", component);
    }

    protected void clearMDC()
    {
        // MDC.clear();
    }

    public void doAppend(Level level, String method, DuccId jobid, String msg, Throwable t)
    {
        DuccLoggingEvent ev = new DuccLoggingEvent(logger, component, level, method, jobid, msg, t, Thread.currentThread().getId(), Thread.currentThread().getName());
        if ( threaded.get() ) {
            events.offer(ev);
        } else {
            doLog(ev);
        }
    }

    public void doAppend(Level level, String method, DuccId jobid, String msg)
    {
        DuccLoggingEvent ev = new DuccLoggingEvent(logger, component, level, method, jobid, msg, null, Thread.currentThread().getId(), Thread.currentThread().getName());
        if ( threaded.get() ) {
            events.offer(ev);
        } else {
            doLog(ev);
        }
    }

    public void fatal(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, location, jobid, formatMsg(args));
        }
    }

    public void fatal(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, location, jobid, formatMsg(args), t);
        }
    }

    public void fatal(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, location, jobid, formatMsg(processId, args));
        }
    }

    public void fatal(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, location, jobid, formatMsg(processId, args), t);
        }
    }
    
    public void debug(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, location, jobid, formatMsg(args));
        } 
    }

    public void debug(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, location, jobid, formatMsg(args), t);
        }
    }
    
    public void debug(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, location, jobid, formatMsg(processId, args));
        } 
    }

    public void debug(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, location, jobid, formatMsg(processId, args), t);
        }
    }
    
    public void error(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, location, jobid, formatMsg(args));
        }
    }

    public void error(String location, DuccId jobid, Throwable t, Object ... args)
    { 
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, location, jobid, formatMsg(args), t);
        }
    }
    
    public void error(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, location, jobid, formatMsg(processId, args));
        }
    }

    public void error(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    { 
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, location, jobid, formatMsg(processId, args), t);
        }
    }
    
    public void info(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, location, jobid, formatMsg(args));
        }
    }

    public void info(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, location, jobid, formatMsg(args), t);
        }
    }
    
    public void info(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, location, jobid, formatMsg(processId, args));
        }
    }

    public void info(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, location, jobid, formatMsg(processId, args), t);
        }
    }
    
    public void trace(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, location, jobid, formatMsg(args));
        }
    }

    public void trace(String location, DuccId jobid, Throwable t, Object ... args)
    {    
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, location, jobid, formatMsg(args), t);
        }
    }
    
    public void trace(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, location, jobid, formatMsg(processId, args));
        }
    }

    public void trace(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {    
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, location, jobid, formatMsg(processId, args), t);
        }
    }
    
    public void warn(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, location, jobid, formatMsg(args));
        }
    }

    public void warn(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, location, jobid, formatMsg(args), t);
        }
    }
    
    public void warn(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, location, jobid, formatMsg(processId, args));
        }
    }

    public void warn(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, location, jobid, formatMsg(processId, args), t);
        }
    }

    //
    
    public void doAppend(Level level, String daemon, String user, String type, String msg)
    {
        DuccLoggingEvent ev = new DuccLoggingEvent(logger, level, daemon, user, type, msg);
        if ( threaded.get() ) {
            events.offer(ev);
        } else {
            doLog(ev);
        }
    }
    
    public void event_info(String daemon, String user, String type, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, daemon, user, type, formatMsg(args));
        }
    }
    
    public void event_warn(String daemon, String user, String type, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, daemon, user, type, formatMsg(args));
        }
    }
    
    /**
     * Stops the logger for the entire process after draining whatever else it might still have.  It's not intended to be restarted.
     */
    public void shutdown()
    {
        if ( threaded.get() ) {
            DuccLoggingEvent ev = new DuccLoggingEvent();
            ev.done = true;
            events.offer(ev);
        }
    }

    class DuccLoggingEvent
    {
        Logger logger;
        String component;
        Level level;
        Object msg;
        Throwable throwable;
        boolean done = false;
        long tid;
        String threadName;
        String method;
        String jobid;
        
        String daemon;
        String user;
        String type;
        
        DuccLoggingEvent() {
        }
        
        DuccLoggingEvent(Logger logger, String component, Level level, String method, DuccId jobid, Object msg, Throwable throwable, long threadId, String threadName)
        {
            this.logger = logger;
            this.component = component.trim();
            this.level = level;
            this.method = method.trim();
            this.jobid = format(jobid);
            this.msg = msg;
            this.throwable = throwable;
            this.tid = threadId;
            this.threadName = threadName.trim();
        }
        
        DuccLoggingEvent(Logger logger, Level level, String daemon, String user, String type, Object msg)
        {
            this.logger = logger;
            this.level = level;
            this.daemon = daemon;
            this.user = user;
            this.type = type;
            this.msg = msg;
        }
    }

    private   static Throwable loggingError = null;
    private   static boolean   disable_logger = false;
    protected static void setThrowable(Throwable t)
    {
        loggingError = t;
    }

    private static void mdc_put(String key, String value) {
    	if(value != null) {
    		MDC.put(key, value);
    	}
    }
    

    private static void mdc_put(String key, long value) {
    	MDC.put(key, value);
    }
    
    /**
     * Common log update for static and threaded modes.
     */
    protected static synchronized void doLog(DuccLoggingEvent ev)
    {
        if ( disable_logger ) return;

        mdc_put("COMPONENT", ev.component);
        mdc_put("TID", ev.tid);
        mdc_put("JID", ev.jobid);
        mdc_put("METHOD", ev.method);
        mdc_put("TNAME", ev.threadName);
        
        mdc_put("DAEMON", ev.daemon);
        mdc_put("USER", ev.user);
        mdc_put("TYPE", ev.type);
        
        try {
            if (ev.throwable == null) {
                ev.logger.log(ev.level, ev.msg);
            } else {
                ev.logger.log(ev.level, ev.msg, ev.throwable);
            }
            if ( loggingError != null ) {
                throw loggingError;
            }
        } catch (Throwable t) {
            loggingError = null;
            if( threaded.get() ) {
            	System.out.println("Disabling logging due to logging exception.");
                disable_logger = true;
                throw new LoggingException("Error writing to DUCC logs", t);
            }
            else {
            	StringWriter errors = new StringWriter();
                t.printStackTrace(new PrintWriter(errors));
                System.out.println(errors.toString());
                System.out.println("Unable to log due to logging exception.");
            }
        }        
    }

    static class DuccLoggingThread
        extends Thread
    {
        public void run()
        {
            while ( true ) {
            	
                DuccLoggingEvent ev = null;
				try {
					ev = events.take();
				} catch (InterruptedException e) {                    
					System.out.println("Logger is interrupted!");
                    continue;
				}

                if ( ev.done ) return;      // we're shutdown
                doLog(ev);
            }
        }
    }

    static class DuccLogErrorHandler
        implements ErrorHandler
    {
    	DuccLogger duccLogger = null;
    	DuccLogErrorHandler(DuccLogger dl)
    	{
    		this.duccLogger = dl;
    	}
    	public void error(String msg) 
        {
            System.err.println("A " + msg);
    	}

    	public void error(String msg, Exception e, int code) 
        {
            System.err.println("B " + msg);
            loggingError = e;
    	}
        
        public void error(String msg, Exception e, int code, LoggingEvent ev) 
        {
            System.err.println("C " + msg);
            loggingError = e;
    	}

    	public void setAppender(Appender appender)
        {
            System.err.println("D");
    	}

    	public void setBackupAppender(Appender appender)
        {
            System.err.println("E");
    	}

        public void setLogger(Logger logger)
        {
            System.err.println("F");
        }

        public void activateOptions()
        {
            System.err.println("G");
        }
    }

}
