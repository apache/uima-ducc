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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;

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
    String host;                                   // Where the instance is scheduled

    ServiceSet sset;                               // handle to the service definitiopn

    JobState state = JobState.Undefined;           // orchestartor state
    String user = null;

    boolean stopped;                               // careful .. this means it was stopped by a stop order from somewhere,
                                                   //   NOT that it's terminating

    String ducc_home = System.getProperty("DUCC_HOME");
    String api_classpath = ducc_home + "/lib/uima-ducc-cli.jar" + ":" + System.getProperty("java.class.path");

    ServiceInstance(ServiceSet sset)
    {
        this.numeric_id = -1;
        this.sset = sset;
        this.stopped = true;
        this.share_id = -1;
        this.host = "<unknown>";
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

    long start(String spec, DuccProperties meta_props)
    {
    	String methodName = "start";

        logger.info(methodName, sset.getId(), "START INSTANCE");
        setStopped(false);
        this.user = meta_props.getProperty("user");

        // Simple use of ducc_ling, just submit as the user.  The specification will have the working directory
        // and classpath needed for the service, handled by the Orchestrator and Job Driver.
        String[] args = {
            System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
            "-u",
            user,
            "--",
            System.getProperty("ducc.jvm"),
            "-cp",
            api_classpath,
            "org.apache.uima.ducc.cli.DuccServiceSubmit",
            "--specification",
            spec
        };
            
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
        Map<String, String> env = pb.environment();
        env.put("DUCC_HOME", System.getProperty("DUCC_HOME"));

        ArrayList<String> stdout_lines = new ArrayList<String>();
        ArrayList<String> stderr_lines = new ArrayList<String>();
		try {
			Process p = pb.start();

            int rc = p.waitFor();
            logger.debug(methodName, null, "DuccServiceSubmit returns with rc", rc);

            // TODO: we should attach these streams to readers in threads because too much output
            //       can cause blocking, deadlock, ugliness.
			InputStream stdout = p.getInputStream();
			InputStream stderr = p.getErrorStream();
			BufferedReader stdout_reader = new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stderr_reader = new BufferedReader(new InputStreamReader(stderr));
			String line = null;
			while ( (line = stdout_reader.readLine()) != null ) {
			    stdout_lines.add(line);
			}

			line = null;
			while ( (line = stderr_reader.readLine()) != null ) {
			    stderr_lines.add(line);
			}

		} catch (Throwable t) {
			// TODO Auto-generated catch block
            logger.error(methodName, null, t);
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

            if ( s.startsWith("Service") && s.endsWith("submitted") ) {
                String[] toks = s.split("\\s");
                try {
                    numeric_id = Long.parseLong(toks[1]);
                    started = true;
                    logger.info(methodName, null, "Request to start service " + sset.getId().toString() + " accepted as service instance ", numeric_id);
                } catch ( NumberFormatException e ) {
                    logger.warn(methodName, null,  "Request to start service " + sset.getId().toString() + " failed, can't interpret response.: " + s);
                }

            }
        }

        if ( ! started ) {
            logger.warn(methodName, null, "Request to start service " + sset.getId().toString() + " failed.");
            meta_props.put("submit-error", submit_buffer.toString());
            sset.log_errors(stdout_lines, stderr_lines);
        } else {
            meta_props.remove("submit-error");
            state = JobState.Received;
        }
        logger.info(methodName, sset.getId(), "START INSTANCE COMPLETE");

        return numeric_id;
    }


    /**
     * This assumes the caller has already verified that I'm a registered service.
     */
    void stop()
    {
        String methodName = "stop";

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
        env.put("DUCC_HOME", System.getProperty("DUCC_HOME"));
            
        ArrayList<String> stdout_lines = new ArrayList<String>();
        ArrayList<String> stderr_lines = new ArrayList<String>();
        int rc = 0;
        try {
            Process p = pb.start();
                
            rc = p.waitFor();
            logger.info(methodName, sset.getId(), "DuccServiceCancel returns with rc", rc);

            if (logger.isTrace() || (rc != 0)) {
                InputStream stdout = p.getInputStream();
                InputStream stderr = p.getErrorStream();
                BufferedReader stdout_reader = new BufferedReader(new InputStreamReader(stdout));
                BufferedReader stderr_reader = new BufferedReader(new InputStreamReader(stderr));
                
                String line = null;
                while ( (line = stdout_reader.readLine()) != null ) {
                    stdout_lines.add(line);
                }
                
                line = null;
                while ( (line = stderr_reader.readLine()) != null ) {
                    stderr_lines.add(line);
                }
            }
                
        } catch (Throwable t) {
            // TODO Auto-generated catch block
            logger.error(methodName, null, t);
        }

        if ( logger.isTrace() || ( rc != 0) ) {
            boolean inhibit_cp = false;
            for ( String s : stdout_lines ) {
                // simple logic to inhibit printing the danged classpath
                if ( inhibit_cp ) {
                    inhibit_cp = false;
                    logger.info(methodName, sset.getId(), "Instance", numeric_id, "<INHIBITED CP>");
                } else {
                    logger.info(methodName, sset.getId(), "Instance", numeric_id, "Stop stdout:", s);
                }
                
                if ( s.indexOf("-cp") >= 0 ) {
                    inhibit_cp = true;
                }
            }
            
            for ( String s : stderr_lines ) {
                logger.info(methodName, sset.getId(), "Instance", numeric_id, "Stop stderr:", s);
            }
        }
    }

}
