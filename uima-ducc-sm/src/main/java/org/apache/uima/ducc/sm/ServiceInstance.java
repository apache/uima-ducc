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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.user.common.QuotedOptions;

/**
* Represent a single instance.
*
* This is a simple class, mostly just a container for the state machine.
*/

class ServiceInstance
	implements SmConstants
{
	private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);

    long   numeric_id;                             // unique numeric ducc-assigned id
    long   share_id;                               // RM's share ID for this instance
    int    instance_id = 0;                        // unique and constant ID assigned by SM to this instance
                                                   // which allows services to know "which" instance they are
                                                   // UIMA-4258

    String host;                                   // Where the instance is scheduled

    ServiceSet sset;                               // handle to the service definitiopn

    JobState state = JobState.Undefined;           // orchestartor state
    String user = null;

    boolean stopped;                               // careful .. this means it was stopped by a stop order from somewhere,
                                                   //   NOT that it's terminating

    String ducc_home = System.getProperty(IDuccUser.EnvironmentVariable.DUCC_HOME.value());
    String api_classpath = ducc_home + "/lib/uima-ducc-cli.jar" + ":" + System.getProperty("java.class.path");

    ServiceInstance(ServiceSet sset)
    {
        this.numeric_id = -1;
        this.sset = sset;
        this.stopped = true;
        this.share_id = -1;
        this.host = "<unknown>";
    }

    // UIMA-4258
    public int getInstanceId()
    {
        return instance_id;
    }

    // UIMA-4258
    public void setInstanceId(int id)
    {
        this.instance_id = id;
    }

    public long getId() {
        return this.numeric_id;
    }

    void setId(long id) {
        this.numeric_id = id;
    }

    public long getShareId()
    {
        return share_id;
    }

    public String getHost()
    {
        return host;
    }

    void setUser(String user)
    {
        this.user = user;
    }

    public void setState(JobState state)
    {
        this.state = state;
    }

    public JobState getState()
    {
        return this.state;
    }

    /**
     * Stopped by some stop order?
     */
    public synchronized boolean isStopped()
    {
        return this.stopped;
    }

    /**
     * On it's way up, or already up, and not stopped for any reason.
     */
    public synchronized boolean isRunning()
    {
        // String methodName = "setState";
		// Received,				// Job has been vetted, persisted, and assigned unique Id
		// WaitingForDriver,		// Process Manager is launching Job Driver
		// WaitingForServices,		// Service Manager is checking/starting services for Job
		// WaitingForResources,	// Scheduler is assigning resources to Job
		// Initializing,			// Process Agents are initializing pipelines
		// Running,				// At least one Process Agent has reported process initialization complete
		// Completing,				// Job processing is completing
		// Completed,				// Job processing is completed
		// Undefined				// None of the above

        switch ( state ) {
            case Completing:
            case Completed:
                return false;
            default:
                return !isStopped();
        }
    }

    synchronized void update(long share_id, String host)
    {
        this.share_id = share_id;
        this.host = host;
    }

    synchronized void setStopped(boolean s)
    {
        this.stopped = s;
    }


//     void setState(DuccWorkJob dwj)
//     {
//         this.state = dwj.getJobState();
//     }

    String[] genArgs(DuccProperties props)
    {
        List<String> args = new ArrayList<String>();

        args.add(System.getProperty("ducc.agent.launcher.ducc_spawn_path"));
        args.add("-u");
        args.add(user);
        args.add("--");
        args.add(System.getProperty("ducc.jvm"));
        args.add("-cp");
        args.add(api_classpath);
        args.add("org.apache.uima.ducc.cli.DuccServiceSubmit");
        args.add("--service_id");
        args.add(sset.getId().toString());

        @SuppressWarnings("rawtypes")
        Enumeration keys = props.propertyNames();
        while ( keys.hasMoreElements() ) {
            String k = (String) keys.nextElement();
            // System.out.println("------ Set argument " + k + " to " + ((String)props.get(k)));
            String v = (String) props.get(k);
            args.add("--" + k);
            if (!k.equals("debug")) {         // Only debug has no value
              args.add(v);
            }
        }
        return args.toArray(new String[args.size()]);
    }

    ArrayList<String> stdout_lines = new ArrayList<String>();
    ArrayList<String> stderr_lines = new ArrayList<String>();
    long start(DuccProperties svc_props, DuccProperties meta_props)
    {
    	String methodName = "start";

        logger.info(methodName, sset.getId(), "START INSTANCE");
        setStopped(false);
        this.user = meta_props.getProperty(IStateServices.SvcMetaProps.user.pname());

        // Simple use of ducc_ling, just submit as the user.  The specification will have the working directory
        // and classpath needed for the service, handled by the Orchestrator and Job Driver.
        String[] args = genArgs(svc_props);

        for ( int i = 0; i < args.length; i++ ) {
            if ( i > 0 && (args[i-1].equals("-cp") ) ) {
                // The classpaths can be just awful filling the logs with junk.  It will end up in the agent log
                // anyway so let's inhibit it here.
                logger.debug(methodName, sset.getId(), "Args[", i, "]: <CLASSPATH>");
            } else {
                logger.debug(methodName, sset.getId(), "Args[", i, "]:", args[i]);
            }
        }

        ProcessBuilder pb = new ProcessBuilder(args);
        StdioListener sin_listener = null;
        StdioListener ser_listener = null;

        Map<String, String> env = pb.environment();
        env.put(IDuccUser.EnvironmentVariable.DUCC_HOME.value(), System.getProperty(IDuccUser.EnvironmentVariable.DUCC_HOME.value()));
        env.put(IDuccUser.EnvironmentVariable.DUCC_ID_SERVICE.value(), Integer.toString(instance_id));  // UIMA-4258

        // Extract the DUCC_UMASK setting and put it ducc_ling's environment UIMA-5328
        // Could use QuotedOprtions to build a map but since we want just one ...
        //        ArrayList<String> envVarList = QuotedOptions.tokenizeList(environment, true);
        final String umaskKey = "DUCC_UMASK";
        String envValue = svc_props.getProperty(IStateServices.SvcRegProps.environment.pname());
        if (envValue != null) {
            List<String> envList = Arrays.asList(envValue.split("\\s+"));   // No need to strip quotes ... !?
            Map<String, String> envMap = QuotedOptions.parseAssignments(envList, 0);
            String umask = envMap.get(umaskKey);
            if (umask != null) {
                env.put(umaskKey, umask);
            }
        }

		try {
			Process p = pb.start();

			InputStream stdout = p.getInputStream();
			InputStream stderr = p.getErrorStream();

            sin_listener = new StdioListener(1, stdout);
            ser_listener = new StdioListener(2, stderr);
            Thread sol = new Thread(sin_listener);
            Thread sel = new Thread(ser_listener);
            sol.start();
            sel.start();

            int rc = p.waitFor();
            logger.debug(methodName, null, "DuccServiceSubmit returns with rc", rc);

            sin_listener.stop();
            ser_listener.stop();
		} catch (Throwable t) {
            logger.error(methodName, sset.getId(), t);
            try {
                sset.setErrorString(t.toString());
            } catch ( Exception e ) {
                logger.warn(methodName, sset.getId(), "Error updating meta properties:", e);
            }
            return -1;
		}

        for ( String s : stderr_lines ) {
            logger.info(methodName, sset.getId(), "Start stderr:", s);
        }

        // That was annoying.  Now search the lines for some hint of the id.
        boolean inhibit_cp = false;
        boolean started = false;
        StringBuffer submit_buffer = new StringBuffer();
        boolean recording = false;
        for ( String s : stdout_lines ) {

            // simple logic to inhibit printing the danged classpath
            if ( inhibit_cp ) {
                inhibit_cp = false;
                logger.info(methodName, sset.getId(), "<INHIBITED CP>");
            } else {
                logger.info(methodName, sset.getId(), "Start stdout:", s);
            }

            if ( s.indexOf("-cp") >= 0 ) {
                inhibit_cp = true;
            }

            if ( recording ) {
                submit_buffer.append(s.trim());
                submit_buffer.append(";");
            }
            if ( s.startsWith("1001 Command launching...") ) {
                recording = true;
                continue;
            }

            // e.g. Service instance 18803 submitted
            if ( s.startsWith("Service") && s.endsWith("submitted") ) {
                String[] toks = s.split("\\s");
                try {
                    numeric_id = Long.parseLong(toks[2]);
                    started = true;
                    logger.info(methodName, null, "Request to start service " + sset.getId().toString() + " accepted as service instance ", numeric_id);
                } catch ( NumberFormatException e ) {
                    try {
                        sset.setErrorString("Request to start service " + sset.getId().toString() + " failed, can't interpret submit response.: " + s);
                    } catch ( Exception ee ) {
                        logger.warn(methodName, sset.getId(), "Error updating meta properties:", ee);
                    }
                    logger.warn(methodName, null,  "Request to start service " + sset.getId().toString() + " failed, can't interpret response.: " + s);
                }

            }
        }

        if ( ! started ) {
            logger.warn(methodName, sset.getId(), "Request to start service " + sset.getId().toString() + " failed.");
            meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), submit_buffer.toString());
            sset.log_errors(stdout_lines, stderr_lines);
        } else {
            meta_props.remove(IStateServices.SvcMetaProps.submit_error.pname());
            state = JobState.Received;
        }
        logger.info(methodName, sset.getId(), "START INSTANCE COMPLETE");

        stdout_lines.clear();
        stderr_lines.clear();

        return numeric_id;
    }


    /**
     * This assumes the caller has already verified that I'm a registered service.
     */
    void stop()
    {
        String methodName = "stop";

        /*
         * Accommodate use of service id and endpoint for information purposes,
         * which informs system event logger when canceling the instance.
         */
        String service_id = sset.getId().toString();
        String service_endpoint = sset.getEndpoint();
        
        setStopped(true);
        String[] args = {
            System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
            "-u",
            user,
            "--",
            System.getProperty("ducc.jvm"),
            "-cp",
            api_classpath,
            "org.apache.uima.ducc.cli.DuccServiceCancel",
            "--id",
            Long.toString(numeric_id),
            "--service_id",
            service_id,
            "--service_request_endpoint",
            service_endpoint,
        };

        for ( int i = 0; i < args.length; i++ ) {
            if ( i > 0 && (args[i-1].equals("-cp") ) ) {
                // The classpaths can be just awful filling the logs with junk.  It will end up in the agent log
                // anyway so let's inhibit it here.
                logger.debug(methodName, sset.getId(), "Instance", numeric_id, "Args[", i, "]: <CLASSPATH>");
            } else {
                logger.debug(methodName, sset.getId(), "Instance", numeric_id, "Args[", i, "]:", args[i]);
            }
        }

        ProcessBuilder pb = new ProcessBuilder(args);
        Map<String, String> env = pb.environment();
        env.put(IDuccUser.EnvironmentVariable.DUCC_HOME.value(), System.getProperty(IDuccUser.EnvironmentVariable.DUCC_HOME.value()));

        pb.redirectOutput(new File("/dev/null"));
        pb.redirectError(new File("/dev/null"));

        int rc = 0;
        try {
            Process p = pb.start();

            rc = p.waitFor();
            logger.info(methodName, sset.getId(), "DuccServiceCancel returns with rc", rc);
        } catch (Throwable t) {
            logger.error(methodName, null, t);
        }
    }

    class StdioListener
        implements Runnable
    {
        InputStream in;
        String tag;
        boolean done = false;
        int which = 0;
        boolean ignore = false;

        StdioListener(int which, InputStream in, boolean ignore)
        {
            this.in = in;
            this.which = which;
            switch ( which ) {
               case 1: tag = "STDOUT: "; break;
               case 2: tag = "STDERR: "; break;
            }
            this.ignore = ignore;
            this.ignore = ignore;
        }

        StdioListener(int which, InputStream in)
        {
            this(which, in, false);
        }

        void stop()
        {
            this.done = true;
        }

        public void run()
        {
            long tid = Thread.currentThread().getId();
            String methodName = "SvcSubmit[" + tid + "]";

            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while ( true ) {
                try {
                    if ( done ) return;
                    String s = br.readLine();
                    if ( logger.isTrace() ) {
                        logger.trace(methodName, sset.getId(), "[]", tag, s);
                    }
                    if ( s == null ) {
                        String msg = tag + "closed, listener returns";
                        logger.info(methodName, sset.getId(), msg);
                        return;
                    }
                    if ( ignore ) continue;  // just discarding it

                    switch ( which ) {
                        case 1:
                            stdout_lines.add(s);
                            break;
                        case 2:
                            stderr_lines.add(s);
                            break;
                    }


				} catch (Exception e) {
                    // if anything goes wrong this guy is toast.
                    logger.error(methodName, sset.getId(), e);
                    return;
				}
            }

        }
    }

}
