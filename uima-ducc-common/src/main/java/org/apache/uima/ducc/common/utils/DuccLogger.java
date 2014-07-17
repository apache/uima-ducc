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
package org.apache.uima.ducc.common.utils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.uima.InternationalizedException;
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
    private static boolean threaded = false;

    private final static String DEFAULT_COMPONENT = "DUCC";
    private static List<Logger> nonDuccLoggers = new ArrayList<Logger>();

    static protected void initLogger()
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
        threaded = false;
    }

    static public void setThreaded()
    {
        threaded = false;;
    }

    public DuccLogger(String claz, String component)
    {
        // initLogger();

        //
        // Try to set component from calling thread if not set.  
        //
        // If all else fails, set it to "DUCC"
        //
    	System.out.println("Creating logger '" + claz + "' with component " + component);
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
                System.out.println(" ===> Configured loggers " + n);
                if ( ! n.startsWith("org.apache.uima.ducc" ) ) {
                    System.out.println("      Special logger: " + n);
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
        // if ( true) return;
    	System.out.println("============ Looking for appenders -----------");
        if ( isDefaultLogger() ) {
            System.out.println(" ---> Skipping appender search for default component");
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
                        System.out.println(" ---> Found appender " + app.getName() + " on logger " + l.getName());
                        for ( Logger ll : nonDuccLoggers ) {     // put the appender on the non-Ducc logger
                            System.out.println(" ---> Add appender " + app.getName() + " to logger " + ll.getName());
                            if ( ll.getAppender(app.getName() ) == null ) {
                                ll.addAppender(app);
                            }
                        }
                    } else {
                        System.out.println(" ---> Skipping non-DUCC appender " + app.getName() + " on logger " + l.getName());
                    }
                }
            } else {
                System.out.println(" ---> No appenders on logger " + l.getName());
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

    protected String formatMsg(String loc, DuccId jid, DuccId pid, Object ... args)
    {
    	String jobId = format(jid);
    	String processId = format(pid);
    	String header = String.format("%s %7s %7s", loc, jobId, processId);
        return formatMsg(loc, header, args);
    }
    
    protected String formatMsg(String loc, DuccId jid, Object ... args)
    {
    	String jobId = format(jid);
    	String header = String.format("%s %7s", loc, jobId);
        return formatMsg(loc, header, args);
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

    protected String formatMsg(String loc, String header, Object ... args)
    {
    	StringBuffer s = new StringBuffer(header);
        for ( Object a : args ) {
            if ( a == null ) a = "<null>"; // avoid null pointers

            s.append(" ");
            //
            // Recognize UIMA exceptions and produce more verbose logging
            //
            if ( a instanceof InternationalizedException ) {
                InternationalizedException e = (InternationalizedException) a;
                s.append(e.toString());
                s.append("\n");
                Object[] arguments = e.getArguments();
                if ( arguments.length > 0 ) {
                    s.append("UIMA exception arguments:\n");
                }
                for ( Object o : arguments ) {
                    if ( o == null ) o = "<null>";  // avoid npe
                    s.append(o.toString());
                    s.append("\n");
                }   

                //
                // Dig in to find the original throw, bypassing all
                // the annoying infrastructure gorp in between.
                //
                Throwable nextCause = e.getCause();
                if(nextCause != null) {
                	while ( nextCause.getCause() != null ) {
                		nextCause = nextCause.getCause();
                	}
                	s.append("Caused by:\n");
                	s.append(nextCause.toString());
                	appendStackTrace(s, nextCause);
                }
            } else if ( a instanceof Throwable ) {
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
    
    private String format(DuccId duccId) {
    	String id;
        if ( duccId == null ) {
            id = defaultId;
        } else {
            id = duccId.toString();
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

    public void doAppend(Level level, String msg, Throwable t)
    {
        DuccLoggingEvent ev = new DuccLoggingEvent(logger, component, level, msg, t, Thread.currentThread().getId(), Thread.currentThread().getName());
        if ( threaded ) {
            events.offer(ev);
        } else {
            doLog(ev);
        }
    }

    public void doAppend(Level level, String msg)
    {
        DuccLoggingEvent ev = new DuccLoggingEvent(logger, component, level, msg, null, Thread.currentThread().getId(), Thread.currentThread().getName());
        if ( threaded ) {
            events.offer(ev);
        } else {
            doLog(ev);
        }
    }

    public void fatal(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, formatMsg(location, jobid, args));
        }
    }

    public void fatal(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, formatMsg(location, jobid, args), t);
        }
    }

    public void fatal(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, formatMsg(location, jobid, processId, args));
        }
    }

    public void fatal(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.FATAL) ) {
            doAppend(Level.FATAL, formatMsg(location, jobid, processId, args), t);
        }
    }
    
    public void debug(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, formatMsg(location, jobid, args));
        } 
    }

    public void debug(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, formatMsg(location, jobid, args), t);
        }
    }
    
    public void debug(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, formatMsg(location, jobid, processId, args));
        } 
    }

    public void debug(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.DEBUG) ) {
            doAppend(Level.DEBUG, formatMsg(location, jobid, processId, args), t);
        }
    }
    
    public void error(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, formatMsg(location, jobid, args));
        }
    }

    public void error(String location, DuccId jobid, Throwable t, Object ... args)
    { 
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, formatMsg(location, jobid, args), t);
        }
    }
    
    public void error(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, formatMsg(location, jobid, processId, args));
        }
    }

    public void error(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    { 
        if ( isLevelEnabled(Level.ERROR) ) {
            doAppend(Level.ERROR, formatMsg(location, jobid, processId, args), t);
        }
    }
    
    public void info(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, formatMsg(location, jobid, args));
        }
    }

    public void info(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, formatMsg(location, jobid, args), t);
        }
    }
    
    public void info(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, formatMsg(location, jobid, processId, args));
        }
    }

    public void info(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.INFO) ) {
            doAppend(Level.INFO, formatMsg(location, jobid, processId, args), t);
        }
    }
    
    public void trace(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, formatMsg(location, jobid, args));
        }
    }

    public void trace(String location, DuccId jobid, Throwable t, Object ... args)
    {    
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, formatMsg(location, jobid, args), t);
        }
    }
    
    public void trace(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, formatMsg(location, jobid, processId, args));
        }
    }

    public void trace(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {    
        if ( isLevelEnabled(Level.TRACE) ) {
            doAppend(Level.TRACE, formatMsg(location, jobid, processId, args), t);
        }
    }
    
    public void warn(String location, DuccId jobid, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, formatMsg(location, jobid, args));
        }
    }

    public void warn(String location, DuccId jobid, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, formatMsg(location, jobid, args), t);
        }
    }
    
    public void warn(String location, DuccId jobid, DuccId processId, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, formatMsg(location, jobid, processId, args));
        }
    }

    public void warn(String location, DuccId jobid, DuccId processId, Throwable t, Object ... args)
    {
        if ( isLevelEnabled(Level.WARN) ) {
            doAppend(Level.WARN, formatMsg(location, jobid, processId, args), t);
        }
    }

    /**
     * Stops the logger for the entire process after draining whatever else it might still have.  It's not intended to be restarted.
     */
    public void shutdown()
    {
        if ( threaded ) {
            DuccLoggingEvent ev = new DuccLoggingEvent(null, null, null, null, null, 0, null);
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
        
        DuccLoggingEvent(Logger logger, String component, Level level, Object msg, Throwable throwable, long threadId, String threadName)
        {
            this.logger = logger;
            this.component = component;
            this.level = level;
            this.msg = msg;
            this.throwable = throwable;
            this.tid = threadId;
            this.threadName = threadName;
        }
    }

    private   static Throwable loggingError = null;
    private   static boolean   disable_logger = false;
    protected static void setThrowable(Throwable t)
    {
        loggingError = t;
    }

    /**
     * Common log update for static and threaded modes.
     */
    protected static synchronized void doLog(DuccLoggingEvent ev)
    {
        if ( disable_logger ) return;

        MDC.put("COMPONENT", ev.component);
        MDC.put("TID", ev.tid);
        MDC.put("TNAME", ev.threadName);
        
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
            System.out.println("Disabling logging due to logging exception.");
            disable_logger = true;
            throw new LoggingException("Error writing to DUCC logs", t);
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
