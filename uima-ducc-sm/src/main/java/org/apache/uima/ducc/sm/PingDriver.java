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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.uima.ducc.common.ServiceStatistics;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;


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
    implements IServiceMeta,
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

    int META_PING_MAX = 32*60*1000;   // 32 minutes ( a shade over 30 ...)
    int meta_ping_rate;
    Object ping_rate_sync = new Object();

    int meta_ping_stability;
    String meta_ping_timeout;
    Thread ping_thread;
    ServiceStatistics service_statistics = null;

    String user;
    String working_directory;
    String log_directory;
    boolean do_log = true;

    boolean shutdown = false;
    
    PingDriver(ServiceSet sset)
    {        
        this.sset = sset;
        DuccProperties job_props = sset.getJobProperties();
        DuccProperties meta_props = sset.getMetaProperties();

        this.endpoint          = meta_props.getStringProperty("endpoint");
        this.user              = meta_props.getStringProperty("user");
        String jvm_args_str    = job_props.getStringProperty("service_ping_jvm_args", "");
        this.ping_class        = job_props.getStringProperty("service_ping_class", null);
        if ( this.ping_class != null ) {     // otherwise it's implicit or submitted and we don't care about any of these
            this.meta_ping_timeout = job_props.getStringProperty("service_ping_timeout");
            this.do_log            = job_props.getBooleanProperty("service_ping_dolog", true);
            this.classpath         = job_props.getStringProperty("service_ping_classpath");
            this.working_directory = job_props.getStringProperty("working_directory");
            this.log_directory     = job_props.getStringProperty("log_directory");
        }

        jvm_args_str = jvm_args_str + " -Dducc.sm.meta.ping.timeout=" + meta_ping_timeout;
        jvm_args_str = jvm_args_str.trim();
        jvm_args = jvm_args_str.split("\\s+");

        this.meta_ping_rate = ServiceManagerComponent.meta_ping_rate;
        this.meta_ping_stability = ServiceManagerComponent.meta_ping_stability;
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
        this.ping_class = dp.getStringProperty("service_ping_ping");
        this.classpath = dp.getStringProperty("service_ping_classpath");
        jvm_args = jvm_args_str.split(" ");
        this.test_mode = true;
    }

    public ServiceStatistics getServiceStatistics()
    {
        return service_statistics;
    }

    public void reference()
    {
        if ( this.ping_class == null ) return;   // internal ping, doesn't need this kludge

        synchronized(ping_rate_sync) {
            meta_ping_rate = 500;
        }
        if ( ping_thread != null ) {
            ping_thread.interrupt();
        }
    }

    synchronized int getMetaPingRate()
    {
        return meta_ping_rate;
    }

    public void run()
    {
    	String methodName = "run";
        if ( this.ping_class == null ) {
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

    void handleStatistics(ServiceStatistics stats)
    {
        String methodName = "handleStatistics";

        this.service_statistics = stats;
        if ( stats == null ) {
            logger.error(methodName, sset.getId(), "Service statics are null!");
            errors++;
        } else {
            if ( service_statistics.isAlive() ) {
                synchronized(this) {
                    sset.setResponsive();
                }
                logger.info(methodName, sset.getId(), "Ping ok: ", endpoint, stats.toString());
                missed_pings = 0;
            } else {
                logger.error(methodName, sset.getId(), "Missed_pings ", missed_pings, "endpoint", endpoint);
                if ( ++missed_pings > meta_ping_stability ) {
                    sset.setUnresponsive();
                    logger.info(methodName, sset.getId(), "Seting state to unresponsive, endpoint",endpoint);
                } else {
                    sset.setWaiting();
                    logger.info(methodName, sset.getId(), "Seting state to waiting, endpoint,", endpoint);
                }                
            }
        }

    }

    public void runAsThread()
    {
    	String methodName = "runAsThread";
        UimaAsPing uap = new UimaAsPing(logger);
        try {
            uap.init(endpoint);
        } catch ( Throwable t ) {
            logger.warn(methodName, sset.getId(), t);
            sset.pingExited();
        }
        while ( ! shutdown ) {
            
            handleStatistics(uap.getStatistics());
            if ( errors > error_threshold ) {
                uap.stop();
                logger.warn(methodName, sset.getId(), "Ping exited because of excess errors: ", errors);
                sset.pingExited();
            }
            
            try {
				Thread.sleep(meta_ping_rate);
			} catch (InterruptedException e) {
                // nothing, if we were shutdown we'll exit anyway, otherwise who cares
			}
            
        }
    }

    public void runAsProcess() 
    {
        String methodName = "run";

        try {
            pinger =  new PingThread();
        } catch ( Throwable t ) {
            logger.error(methodName, sset.getId(), "Cannot start listen socket, pinger not started.", t);
            sset.setUnresponsive();
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
        for ( String s : jvm_args) {
            arglist.add(s);
        }
        arglist.add("-cp");
        arglist.add(System.getProperty("java.class.path") + ":" + classpath);
        arglist.add("org.apache.uima.ducc.sm.ServicePingMain");
        arglist.add("--class");
        arglist.add(ping_class);
        arglist.add("--endpoint");
        arglist.add(endpoint);
        arglist.add("--port");
        arglist.add(Integer.toString(port));
        
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
            sset.setUnresponsive();
            return;
        }
        
        int rc;
        while ( true ) {
            try {
                rc = ping_main.waitFor();
                logger.debug(methodName, sset.getId(), "Pinger returns rc ", rc);
                sset.pingExited();
                break;
            } catch (InterruptedException e2) {
                // nothing
            }
        }
		
		pinger.stop();
        sin_listener.stop();
        ser_listener.stop();
    }

    public void stop()
    {
        shutdown = true;
        if ( this.ping_class != null ) {
            if ( pinger       != null ) pinger.stop();
            if ( sin_listener != null ) sin_listener.stop();
            if ( ser_listener != null ) ser_listener.stop();
            if ( ping_main    != null ) ping_main.destroy();
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
            this.done = true;
        }

        public void run()
        {
        	String methodName = "PingThread.run()";
            try {

                Socket sock = server.accept();
                // Socket sock = new Socket("localhost", port);
                sock.setSoTimeout(5000);
                OutputStream out = sock.getOutputStream();
                InputStream  in =  sock.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(in);
                
                ping_ok = false;         // we expect the callback to change this
				while ( true ) {
                    synchronized(this) {
                        if ( done ) return;
                    }

                    if ( errors > error_threshold ) {
                        stop();
                    }

                    // Ask for the ping
                    try {
                        logger.trace(methodName, sset.getId(), "PingDriver: ping OUT");
                        out.write('P');
                        out.flush();
                    } catch (IOException e1) {
                        logger.error(methodName, sset.getId(), e1);
                        errors++;
                    }
                    
                    // Try to read the response
                    handleStatistics((ServiceStatistics) ois.readObject());

                    // This kliudge is required because we have to insure that pings aren't too frequent or ActiveMQ will get OutOfMemory errors.
                    // So we do exponential backoff.  If a new job references the service we set the ping rate to something frequent for a while
                    // to insure waiting jobs don't have to wait too long ( done in PingDriver object ).
                    int my_ping_rate;
                    synchronized(ping_rate_sync) {
                        my_ping_rate = meta_ping_rate;
                        if ( (meta_ping_rate < META_PING_MAX) && ( missed_pings == 0) ) {
                            // double, if not at max rate, and pinging is working 
                            meta_ping_rate = Math.min(META_PING_MAX, meta_ping_rate * 2);
                        }
                    }

                    // Wait a bit for the next one
                    try {
                        // logger.info(methodName, sset.getId(), "SLEEPING", my_ping_rate, "ms", sset.toString());
                        Thread.sleep(my_ping_rate);
                        // logger.info(methodName, sset.getId(), "SLEEP returns", sset.toString());
                    } catch (InterruptedException e) {
                        // nothing
                    }

                }
			} catch (IOException e) {
                logger.error(methodName, sset.getId(), "Error receiving ping", e);
                errors++;
			} catch (ClassNotFoundException e) {
                logger.error(methodName, sset.getId(), "Input garbled:", e);
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
            String methodName = "StdioListener.run";

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
