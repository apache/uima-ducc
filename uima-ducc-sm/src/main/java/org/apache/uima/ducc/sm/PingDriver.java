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
package org.apache.uima.ducc.sm;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.uima.ducc.cli.AServicePing;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;


/**
 * This runs the watchdog thread for custom service pingers.
 *
 * It spawns a process, as the user, which in turn will instantiate an object which extends
 * AServiceMeta to implement the pinger.
 *
 * The processes communicate via a pipe: every ping interval the meta puts relevent information onto its
 * stdout:
 *     0|1 long long
 * The first token is 1 if the ping succeeded, 0 otherwise.
 * The second token is the total cumulative work executed by the service.
 * The third token is the current queue depth of the service.       
 */

class PingDriver
    implements IServiceMeta,          // (extends runnable )
               SmConstants
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);	

    String[] jvm_args;
    String endpoint;
    String ping_class;
    String ping_arguments;
    String classpath;
    boolean ping_ok;

    int missed_pings = 0;            // didn't ping in specified time, but no error thrown
    int errors = 0;                  // error, no good
    int error_threshold = 5;         // max errors before we die

    ServiceSet sset;
    boolean test_mode = false;

    Process ping_main;

    StdioListener sin_listener = null;
    StdioListener ser_listener = null;
    PingThread pinger = null;

    int meta_ping_rate;              // ducc.properties configured ping rate
    int meta_ping_stability;         // ducc.properties number of missed pings before setting service unresponive
    long meta_ping_timeout;          // how long we wait for pinger to return when requesing a ping
    Thread ping_thread;              // thread to manage external process pingers
    boolean internal_ping = true;    // if true, use default UIMA-AS pinger in thread inside SM propert

    IServiceStatistics service_statistics = null;

    String user;
    String working_directory;
    String log_directory;
    boolean do_log = true;

    boolean shutdown = false;
    PingStopper pingStopper = null;
    Timer timer = null;
    
    ServiceState pingState = ServiceState.Waiting;
    
    PingDriver(ServiceSet sset)
    {        
        this.sset = sset;
        DuccProperties job_props = sset.getJobProperties();
        DuccProperties meta_props = sset.getMetaProperties();

        // establish the default pinger, then see if another pinger is specified and set it.        
        this.ping_class        = System.getProperty("ducc.sm.default.monitor.class", "org.apache.uima.ducc.cli.UimaAsPing");
        this.ping_class        = job_props.getStringProperty("service_ping_class", this.ping_class);

        // If the pinger is registered with us we can pick up (and trust) the registered defaults.  Read the registration now.
        DuccProperties ping_props = findRegisteredPinger(this.ping_class);
        if ( ping_props == null ) {          // this is an internal or system error of some sort
            throw new IllegalStateException("Cannot start pinger.");
        } else {
            this.internal_ping = ping_props.getBooleanProperty("internal", false);

            // One more resolution, in case the class name is actually the name of a registered pinger
            String real_class  = ping_props.getProperty("service_ping_class");
            if ( real_class != null ) {
                this.ping_class = real_class;
            }
            logger.info("<ctr>", sset.getId(), "Using ping class", this.ping_class); 
        }
        
        this.endpoint          = meta_props.getStringProperty("endpoint");
        this.user              = meta_props.getStringProperty("user");

        this.ping_arguments    = resolveStringProperty("service_ping_arguments", ping_props, job_props, null);
        String jvm_args_str    = resolveStringProperty("service_ping_jvm_args" , ping_props, job_props, "");
        
        this.meta_ping_timeout = resolveIntProperty    ("service_ping_timeout", ping_props, job_props, ServiceManagerComponent.meta_ping_timeout);
        this.do_log            = resolveBooleanProperty("service_ping_dolog", ping_props, job_props, false);
        this.classpath         = resolveStringProperty ("service_ping_classpath", ping_props, job_props, System.getProperty("java.class.path"));
        this.working_directory = resolveStringProperty ("working_directory", ping_props, job_props, null); // cli always puts this int job props, no default 

        this.log_directory     = resolveStringProperty ("log_directory", ping_props, job_props, null);     // cli always puts this int job props, no default 

        jvm_args_str = jvm_args_str.trim();
        if ( jvm_args_str.equals("") ) {
            jvm_args = null;
        } else {
            jvm_args = jvm_args_str.split("\\s+");
        }

        // global, not customizable per-pinger
        this.meta_ping_rate      = ServiceManagerComponent.meta_ping_rate;
        this.meta_ping_stability = ServiceManagerComponent.meta_ping_stability;
    }

    //
    // If the class is registered, read it into ducc properties and return it.  Else return an
    // empty ducc properties.  The resolver will deal with the emptiness.
    //
    protected DuccProperties findRegisteredPinger(String cls)
    {
    	String methodName = "find RegisteredPinger";
        DuccProperties answer = new DuccProperties();
        File f = new File(System.getProperty("DUCC_HOME") + "/resources/service_monitors/" + cls);
        if ( f.exists () ) {
            try {
				answer.load(f.getCanonicalPath());
                logger.info(methodName, sset.getId(), "Loading site-registered service monitor from", cls);
			} catch (Exception e) {
                logger.error(methodName, sset.getId(), "Cannot load site-registered service monitor", f.getName(), e);
                return null;
			}
        }
        return answer;        
    }

    // this resolves the prop in either of the two props files and expands ${} against System props, which include
    // everything in ducc.properties
    protected String resolveStringProperty(String prop, DuccProperties ping_props, DuccProperties job_props, String deflt)
    {
        if ( internal_ping ) {
            // internal ping only gets to adjust the ping tuning parameters
            if ( ! prop.equals("service_ping_arguments") ) return ping_props.getStringProperty(prop, deflt);
        } 

        prop = prop.trim();
        //
        // Search order: first,  what is registered to the service, 
        //               second, what is registered to the site-registered pinger
        //               third,  the passed-in default
        //
        String val = job_props.getProperty(prop);
        if ( val == null ) {
            val = ping_props.getProperty(prop);
        }

        if ( val == null ) {
            val = deflt;
        }

        if ( val != null ) val = val.trim();
        return val;
    }

    protected int resolveIntProperty(String prop, DuccProperties ping_props, DuccProperties job_props, int deflt)
    {
        String val = resolveStringProperty(prop, ping_props, job_props, null);
        return (val == null ? deflt : Integer.parseInt(val));
    }

    protected boolean resolveBooleanProperty(String prop, DuccProperties ping_props, DuccProperties job_props, boolean deflt)
    {
        String val = resolveStringProperty(prop, ping_props, job_props, Boolean.toString(deflt));
        return ( val.equalsIgnoreCase("True") ||
                 val.equalsIgnoreCase("true") );
    }

    /**
     * Test from main only
     */
    PingDriver(String props)
    {        
        DuccProperties dp = new DuccProperties();
        try {
			dp.load(props);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        this.endpoint = dp.getStringProperty("endpoint");
        String jvm_args_str = dp.getStringProperty("service_ping_jvm_args", "");
        this.ping_class = dp.getStringProperty("service_ping_class");
        this.classpath = dp.getStringProperty("service_ping_classpath");
        jvm_args = jvm_args_str.split(" ");
        this.test_mode = true;
    }


    /**
     * Used by the ServiceSet state machine.
     */
    public ServiceState getServiceState()
    {
        return this.pingState;
    }

    /**
     * Used by the ServiceSet state machine for messages
     */
    public long getId()
    {
        return 0;
    }

    public JobState getState()
    {
    	String methodName = "getState";
        switch ( pingState ) {
            case Available:
                return JobState.Running;
            case Stopped:
                return JobState.Completed;
            case Waiting:
                return JobState.Initializing;  // not really, but not used. don't have or need a better alternative.
            default:
                logger.error(methodName, sset.getId(), "Unexpected state in Ping driver:", pingState);
                return JobState.Completed;
        }
    }

    public void setState(JobState s) 
    {
        // nothing
    }

    public IServiceStatistics getServiceStatistics()
    {
        return service_statistics;
    }
        
    synchronized int getMetaPingRate()
    {
        return meta_ping_rate;
    }

    public void run()
    {
    	String methodName = "run";

        if ( internal_ping ) {
            // This is the default ping driver, as configured in ducc.propeties, to be run in
            // an in-process thread
            logger.info(methodName, sset.getId(), "Starting INTERNAL ping.");
            runAsThread();
            logger.info(methodName, sset.getId(), "Ending INTERNAL ping.");
        } else {
            // The user specified a pinger, run it as an extranal process under that user's identity
            logger.info(methodName, sset.getId(), "Starting EXTERNAL ping.");
            runAsProcess();
            logger.info(methodName, sset.getId(), "Ending EXTERNAL ping.");
        }

    }

    void handleResponse(Pong response)
    {
        String methodName = "handleStatistics";

        this.service_statistics = response.getStatistics();
        if ( service_statistics == null ) {
            logger.error(methodName, sset.getId(), "Service statics are null!");
            errors++;
        } else {
            if ( service_statistics.isAlive() ) {
                pingState = ServiceState.Available;
                logger.info(methodName, sset.getId(), "Ping ok: ", endpoint, service_statistics.toString());
                missed_pings = 0;
            } else {
                logger.error(methodName, sset.getId(), "Missed_pings ", ++missed_pings, "endpoint", endpoint, service_statistics.toString());
                if ( missed_pings > meta_ping_stability ) {
                    pingState = ServiceState.Waiting;
                }
            }
        }
        
        sset.signalRebalance(response.getAdditions(), response.getDeletions(), response.isExcessiveFailures());
    }
    
    AServicePing loadInternalMonitor()
     	throws ClassNotFoundException,
                IllegalAccessException,
                InstantiationException,
                MalformedURLException
    {
        if ( classpath == null ) {
            @SuppressWarnings("unchecked")
                Class<AServicePing> cl = (Class<AServicePing>) Class.forName(ping_class);
            return (AServicePing) cl.newInstance();
        } else {
            String[] cp_elems = classpath.split(":");
            URL[]    cp_urls = new URL[cp_elems.length];
            
            for ( int i = 0; i < cp_elems.length; i++ ) {
                cp_urls[i] = new URL("file://" + cp_elems[i]);                
            }
            @SuppressWarnings("resource")
                URLClassLoader l = new URLClassLoader(cp_urls);
            @SuppressWarnings("rawtypes")
                Class loaded_class = l.loadClass(ping_class);
            l = null;
            return (AServicePing) loaded_class.newInstance();
        }
    }

    void runAsThread()
    {
        long tid = Thread.currentThread().getId();

    	String methodName = "runAsThread[" + tid + "]";
        AServicePing pinger = null;
        Properties props = new Properties();

		try {
			pinger = loadInternalMonitor();
		} catch (ClassNotFoundException e1) {
            logger.error(methodName, sset.getId(), "Cannot load pinger: ClassNotFoundException(", ping_class, ")");
            return;
		} catch (IllegalAccessException e1) {
            logger.error(methodName, sset.getId(), "Cannot load pinger: IllegalAccessException(", ping_class, ")");
            return;
		} catch (InstantiationException e1) {
            logger.error(methodName, sset.getId(), "Cannot load pinger: InstantiationException(", ping_class, ")");
            return;
		} catch ( MalformedURLException e1) {
            logger.error(methodName, sset.getId(), "Cannot load pinger: Cannot form URLs from classpath entries(", ping_class, ")");
            return;		
		}

        try {
            props.setProperty("total-instances", "" + sset.countImplementors());
            props.setProperty("active-instances", "" + sset.getActiveInstances());
            props.setProperty("references", "" + sset.countReferences());
            props.setProperty("run-failures", "" + sset.getRunFailures());
            props.setProperty("monitor-rate", "" + meta_ping_rate);
            props.setProperty("service-id", "" + sset.getId().getFriendly());

            pinger.setSmState(props);
            pinger.init(ping_arguments, endpoint);
            while ( ! shutdown ) {
                
                Pong pr = new Pong();

                pr.setStatistics(pinger.getStatistics());
                pr.setAdditions (pinger.getAdditions());
                pr.setDeletions (pinger.getDeletions());
                pr.setExcessiveFailures(pinger.isExcessiveFailures());

                handleResponse(pr);
                if ( errors > error_threshold ) {
                    pinger.stop();
                    logger.warn(methodName, sset.getId(), "Ping exited because of excess errors: ", errors);
                    break;
                }
                
                try {
                    Thread.sleep(meta_ping_rate);
                } catch (InterruptedException e) {
                    // nothing, if we were shutdown we'll exit anyway, otherwise who cares
                }                
            }
        } catch ( Throwable t ) {
            logger.warn(methodName, sset.getId(), t);
        }

        pinger = null;
        sset.pingExited(errors, this);
    }

    public void runAsProcess() 
    {
        long tid = Thread.currentThread().getId();
        String methodName = "runAsProcess[" + tid + "]";

        String cp = classpath;
        String dh = System.getProperty("DUCC_HOME");

        // we need the sm jar and the cli jar ... dig around in the runtime to try to find them
        File libdir = new File(dh + "/lib/uima-ducc");
        String[] jars = libdir.list();
        for ( String j : jars ) {
            if ( j.contains("ducc-sm") ) {
                cp = cp + ":" + dh + "/lib/uima-ducc/" + j;
                continue;
            }
            if ( j.contains("ducc-cli") ) {
                cp = cp + ":" + dh + "/lib/uima-ducc/" + j;
                continue;
            }

        }
        //cp = cp + ":" + dh + "/lib/uima-ducc/ducc-sm.jar";
        //''cp = cp + ":" + dh + "/lib/uima-ducc/uima-ducc-cli-1.0.0-SNAPSHOT.jar";

        try {
            pinger =  new PingThread();
        } catch ( Throwable t ) {
            logger.error(methodName, sset.getId(), "Cannot start listen socket, pinger not started.", t);
            pingState = ServiceState.Stopped;
            return;
        }
        int port = pinger.getPort();

        ping_thread = new Thread(pinger);
        ping_thread.start();                            // sets up the listener, before we start the the external process

        ArrayList<String> arglist = new ArrayList<String>();
        if ( ! test_mode ) {
            arglist.add(System.getProperty("ducc.agent.launcher.ducc_spawn_path"));
            arglist.add("-u");
            arglist.add(user);
            arglist.add("-w");
            arglist.add(working_directory);
            if ( do_log ) {
                arglist.add("-f");
                arglist.add(log_directory + "/services/ping/" + sset.getId());
            }
            arglist.add("--");
        }

        arglist.add(System.getProperty("ducc.jvm"));
        if ( jvm_args != null ) {
            for ( String s : jvm_args) {
                arglist.add(s);
            }
        }
        arglist.add("-cp");
        arglist.add(cp);
        //arglist.add("-Xmx100M");
        arglist.add("-Dcom.sun.management.jmxremote");
        arglist.add("org.apache.uima.ducc.sm.ServicePingMain");
        arglist.add("--class");
        arglist.add(ping_class);
        arglist.add("--endpoint");
        arglist.add(endpoint);
        arglist.add("--port");
        arglist.add(Integer.toString(port));

        if( ping_arguments != null ) {
            arglist.add("--arguments");
            arglist.add(ping_arguments);
        }
        
        int i = 0;
        for ( String s : arglist) {
            logger.debug(methodName, sset.getId(), "Args[", i++,"]:  ", s);
        }
        ProcessBuilder pb = new ProcessBuilder(arglist);
        
        //
        // Establish our pinger
        //
        InputStream stdout = null;
        InputStream stderr = null;
        try {
            ping_main = pb.start();
            stdout = ping_main.getInputStream();
            stderr = ping_main.getErrorStream();
            
            sin_listener = new StdioListener(1, stdout);
            ser_listener = new StdioListener(2, stderr);
            Thread sol = new Thread(sin_listener);
            Thread sel = new Thread(ser_listener);
            sol.start();
            sel.start();
        } catch (Throwable t) {
            logger.error(methodName, sset.getId(), "Cannot establish ping process:", t);
            pingState = ServiceState.Stopped;
            return;
        }
        
        int rc;
        while ( true ) {
            try {
                rc = ping_main.waitFor();
                ping_main = null;

                if ( pingStopper != null ) {
                    pingStopper.cancel();
                    pingStopper = null;
                    logger.info(methodName, sset.getId(), "Pinger returned, pingStopper is canceled.");
                }

                logger.info(methodName, sset.getId(), "Pinger returns rc ", rc);
                sset.pingExited(rc, this);
                break;
            } catch (InterruptedException e2) {
                // nothing
            }
        }
		
		// pinger.stop();
        sin_listener.stop();
        ser_listener.stop();
    }

    public void stop()
    {
        shutdown = true;
        if ( !internal_ping ) {
            if ( pinger != null ) pinger.stop();
            pingStopper = new PingStopper();

            if ( timer == null ) {
                timer = new Timer();
            }
            timer.schedule(pingStopper, 60000);
        }
    }

    private class PingStopper
        extends TimerTask
    {
        PingStopper()
        {        
            String methodName = "PingStopper.init";
            logger.debug(methodName, sset.getId(), "Wait for pinger to exit:", 60000);
        }

        public void run()
        {
            String methodName = "PingStopper.run";
            logger.debug(methodName, sset.getId(), "PingStopper kills reluctant pinger");
            if ( ping_main != null )  ping_main.destroy();
        }
    }


    class PingThread
        implements Runnable
    {
        ServerSocket server;
        int port = -1;
        boolean done = false;

        PingThread()
            throws IOException
        {
            this.server = new ServerSocket(0);
            this.port = server.getLocalPort();
		}

        int getPort()
        {
            return this.port;
        }

        synchronized void stop()
        {
        	String methodName = "stop";
            logger.info(methodName, sset.getId(), "Pinger stopping: set done = true");
            this.done = true;
        }

        public void run()
        {
            long tid = Thread.currentThread().getId();
        	String methodName = "XtrnPingThread.run[" + tid + "]";
        	try {

                Socket sock = server.accept();
                // Socket sock = new Socket("localhost", port);
                sock.setSoTimeout(meta_ping_rate);                     // don't timout faster than ping rate
                OutputStream outs = sock.getOutputStream();
                InputStream  in =  sock.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(in);
                ObjectOutputStream oos = new ObjectOutputStream(outs);
                Properties props = new Properties();

                ping_ok = false;         // we expect the callback to change this
				while ( true ) {
                    synchronized(this) {
                        if ( done ) {
                            // Ask for the ping
                            try {
                                logger.info(methodName, sset.getId(), "ExtrnPingDriver: send QUIT to pinger.");
                                oos.writeObject(new Ping(true, props));
                                oos.flush();
                                //oos.reset();
                            } catch (IOException e1) {
                                logger.error(methodName, sset.getId(), e1);
                            }
                            logger.info(methodName, sset.getId(), "ExtrnPingDriver: QUIT is sent and flushed; thread exits.");
                            // gc will close all the descriptors and handles
                            //ois.close();
                            //oos.close();
                            //in.close();                            
                            //sock.close();
                            return;
                        }
                    }

                    // Ask for the ping
                    try {
                        logger.info(methodName, sset.getId(), "ExtrnPingDriver: ping OUT");
                        props.setProperty("total-instances" , "" + sset.countImplementors());
                        props.setProperty("active-instances", "" + sset.getActiveInstances());
                        props.setProperty("references"      , "" + sset.countReferences());
                        props.setProperty("run-failures"    , "" + sset.getRunFailures());
                        props.setProperty("monitor-rate"    , "" + meta_ping_rate);
                        props.setProperty("service-id"      , "" + sset.getId().getFriendly());

                        oos.writeObject(new Ping(false, props));
                        oos.flush();
                        oos.reset();
                    } catch (IOException e1) {
                        logger.error(methodName, sset.getId(), e1);
                        errors++;
                        return;
                    }
                    
                    // Try to read the response
                    try {
                        Pong resp = (Pong) ois.readObject();
                        logger.info(methodName, sset.getId(), "ExtrnPingDriver: ping RECEIVED");
                        handleResponse(resp);
                        logger.info(methodName, sset.getId(), "ExtrnPingDriver: ping HANDLED");
                    } catch (IOException e1) {
                        logger.warn(methodName, sset.getId(), "ExtrnPingDriver: Error receiving ping:", e1);
                        errors++;
                        return;
                    }

                    // Wait a bit for the next one
                    try {
                        logger.info(methodName, sset.getId(), "ExtrnPingDriver: SLEEPING", meta_ping_rate, "ms", sset.toString());
                        Thread.sleep(meta_ping_rate);
                        logger.info(methodName, sset.getId(), "ExtrnPingDriver: SLEEP returns", sset.toString());
                    } catch (InterruptedException e) {
                        logger.info(methodName, sset.getId(), e);
                    }

                }
			} catch (IOException e) {
                logger.error(methodName, sset.getId(), "ExtrnPingDriver: Error receiving ping", e);
                errors++;
			} catch (ClassNotFoundException e) {
                logger.error(methodName, sset.getId(), "ExtrnPingDriver: Input garbled:", e);
                errors++;
			}
        }       
    }

    class StdioListener
        implements Runnable
    {
        InputStream in;
        String tag;
        boolean done = false;

        StdioListener(int which, InputStream in)
        {
            this.in = in;
            switch ( which ) {
               case 1: tag = "STDOUT: "; break;
               case 2: tag = "STDERR: "; break;
            }
        }

        void stop()
        {
            this.done = true;
        }

        public void run()
        {
            if ( done ) return;
            long tid = Thread.currentThread().getId();
            String methodName = "StdioListener.run[" + tid + "]";

            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while ( true ) {
                try {
                    String s = br.readLine();
                    if   ( test_mode ) System.out.println(tag + s);
                    else logger.info(methodName, sset.getId(), tag, s);

                    if ( s == null ) {
                        String msg = tag + "closed, listener returns";
                        if   ( test_mode ) System.out.println(msg);
                        else logger.info(methodName, sset.getId(), msg);
                        return;
                    }

				} catch (IOException e) {
                    // if anything goes wrong this guy is toast.
                    if   ( test_mode) e.printStackTrace();
                    else logger.error(methodName, sset.getId(), e);
                    return;
				}
            }

        }
    }

    public static void main(String[] args)
    {
        // arg0 = amqurl = put into -Dbroker.url
        // arg1 = endpoint - pass to ServicePingMain
        // call ServicePingMain --class org.apache.uima.ducc.sm.PingTester --endpoint FixedSleepAE_1
        //    make sure test.jar is in the classpath
        PingDriver csm = new PingDriver(args[0]);
        csm.run();
    }

}
