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

import java.util.Map;

import org.apache.uima.ducc.common.IServiceStatistics;

/**
 * Abstraction for service pinger.
 */

public abstract class AServicePing
{
    protected int[] failure_window = null;      // tracks consecutive failures within a window
    protected int failure_cursor = 0;           // cursor to track failures within the current window

    /**
     *  This is the total number of instance failures since the SM or pinger was last started.
     */
    protected int total_failures = 0;           // current total run failures. usually monotonically increasing.
    /**
     * This is the total number of instance failures allowed within the failure window.
     */
    protected int failure_max = 3;              // max allowed failures within any given window

    /**
     * This is the time, in minutes, over which the failure window is implemented.
     */
    protected int failure_window_period = 30;   // 30 minutes. overridden at first ping

    protected int failure_window_size = failure_window_period;  // assume 1 ping per minute

    /**
     * This is the time between pings, in minutes.
     */
    protected int monitor_rate = 1;             // ping rate, in minutes, min 1 used for calculations

    /**
     * This indicates whether the service's autostart flag is enabled or disabled.
     */
    protected boolean autostart_enabled;        // indicates whether autostart is currently enable for this pinger

    /**
     * This is the time/date the service was last used.  If 0, the time is either unknown or the service has
     * never been used by clients.  It is persisted by the SM over restarts.
     */
    protected long last_use = 0;                // From SM on init, the last known usage of this service
                                                // according to the meta file.  During runtime, implementors
                                                // may update update it which causes the meta to be updated.

    /**
     * This specifies whether the service log is requested to be enabled.
     */
    protected boolean log_enabled = false;

    /**
     * This is the unique DUCC_assigned ID of the service.
     */
    protected long service_id = 0;    

    /**
     * This is a map containing current service state, passed in from the SM on every ping.  
     * See {@link org.apache.uima.ducc.cli.AServicePing#getSmState()} for details of the map.
     */
    protected Map<String, Object> smState;

    /**
     * This is a map containing the initialization state for the service, passed in only
     * once, during pinger initialization.  Its fields are set into primitive fields
     * in this class.  The map itself isn't directly used by implementors.
     */
    protected Map<String, Object> initializationState;

    /**
     * When the pinger is run as a thread inside the SM, this logger is used to
     * join the ping log with the SM log.  When run as a process, 
     # the {@link org.apache.uima.ducc.cli.AServicePing#doLog(String, Object...) } method
     * writes to stdout which is directed to
     * the declared service log directory by the infrastructore.
     */
    protected org.apache.uima.ducc.common.utils.DuccLogger duccLogger;

    /**
     * Called by the ping driver, to pass in useful things the pinger may want.
     *
     * @param arguments This is passed in from the service specification's
     *                  service_ping_arguments string.
     *
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     */
    public abstract void init(String arguments, String endpoint) throws Exception;

    /**
     * <p>
     * Called by the ping driver to initialize static information about the service and
     * pinger.  This method calls the public init() method and is not intended for public
     * consumption.
     * </p>
     *
     * <p>
     * This method initializes the following state prior to invoking init(String, String):
     * </p>
     *
     * <xmp>
     * VAR NAME               TYPE         MEANING
     * ------------------     --------     ---------------------------------------------
     * monitor_rate           int          Ping period, in minutes.
     * service_id             long         DUCC ID of the service being monitored
     * log_enabled            boolean      Is the service registered with log enabled?
     * failure_max            int          Registered max consecutive failures
     * failure_window_size    int          The window, in terms of minutes, in which
     *                                     'failure-max' errors indicates excessive
     *                                     instance failures.
     * autostart_enabled      boolean      Is the service registered with autostart on?
     * last_use               long         When was the last known use of this service
     *                                     before it was (re)started?
     *
     * </xmp>
     *
     * @param arguments This is passed in from the service specification's
     *                  service_ping_arguments string.
     *
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     *
     * @param initState Properties file with static data about the service and 
     *                  pinger.
     */
    public void init(String arguments, String endpoint, Map<String, Object> initState)
        throws Exception
    {

        this.initializationState = initState;

        monitor_rate          = (Integer) initializationState.get("monitor-rate");        
        service_id            = (Long)    initializationState.get("service-id") ;       
        log_enabled           = (Boolean) initializationState.get("do-log");        
        failure_max           = (Integer) initializationState.get("failure-max");        
        failure_window_period = (Integer) initializationState.get("failure-window");
        autostart_enabled     = (Boolean) initializationState.get("autostart-enabled");
        last_use              = (Long)    initializationState.get("last-use");

        double  calls_per_minute = 60000.00 / monitor_rate;
        failure_window_size = (int) ( ((double)failure_window_period) * calls_per_minute);

        failure_window = new int[failure_window_size];
        failure_cursor = 0;
        init(arguments, endpoint);
    }

    /**
     * Stop is called by the ping wrapper when it is being killed.  Implementors may optionally
     * override this method with conenction shutdown code.
     */
    public abstract void stop();

    /**
     * Returns the object with application-derived health and statistics.
     *
     * @return an object that implements {@link org.apache.uima.ducc.common.IServiceStatistics} containing the basic
     *         service health information for use by SM and display in the Web Server.
     */
    public abstract IServiceStatistics getStatistics();

    /**
     * Current state of the monitored service is passed in here.
     * NOTE: Used for SM to Ping/Monitor communicaiton only.
     */    
    public void setSmState(Map<String, Object> props)
    {
        smState = props;
    }

    /**
     * <p>
     * Getter of the service state;  Implementors may just access it directly if they want.
     * Access the state passed to the ping/monitor from SM:
     * </p>
     * <xmp>
     * KEY                  Object Type       MEANING
     * ----------------     -------------     ------------------------------------------------------------------
     * all-instances        Long[]            DUCC Ids of all running instances (may not all be in Runing state)
     * active-instances     Long[]            DUCC Ids of all instances that are Running
     * autostart-enabled    Boolean           Current state of service autostart
     * references           Long[]            DUCC Ids of all jobs referencing this service
     * run-failures         Integer           Total run failures since the service was started
     * </xmp>
     *
     * @return A Map<String, Object> of string-key to Object containing dynamic information from the SM.  Callers
     *        must cast the value to the correct type as shown below.
     */
    public Map<String, Object> getSmState() 
    {
        return smState;
    }

    /**
     * <p>
     * Called by the service manager to query the number of additional needed service instances.
     * </p>
     *
     * <p>
     * Implementing ping/monitors override this method to request additional instances.
     * </p>
     *
     * @return the number of new instances of the service to start.
     */
    public int getAdditions()
    {
        return 0;
    }

    /**
     * <p>
     * Called by the service manager to retrieve the specific service instances
     * to stop.
     * </p>
     *
     * <p>
     * Implementing ping/monitors return the specific IDs of service processes to
     * be terminated by DUCC.  The IDs are a subset of the IDS found in the
     * 'all-instances' map from getSmState();
     * </p>
     *
     * @return a Long[] array of service instance IDs to terminate.
     */
    public Long[] getDeletions()
    {
        return null;   
    }

    /**
     * <p>
     * The SM queries the ping/monitors autostart on return from each ping.  The default is
     * to return the same value that came in on the ping.  
     * </p>
     *
     * <p>
     * Implementing ping/monitors may override
     * this behaviour to dynanically enable or disable autostart.
     * </p>
     *
     * <p>
     * It is useful to disable autostart if a pinger detects that a service has been
     * idle for a long time and it wants to shrink the number of live instances
     * below the autostart value.  If autostart is not disabled it the number of
     * instances will not be allowed to shrink to 0.
     * </p>
     *
     * @return true if the service should be marked for autostart, and false otherwise.
     */
    public boolean isAutostart()
    {
        if ( smState== null ) {
            return (Boolean) initializationState.get("autostart-enabled");   // no ping yet, return the initial value
        } else {
            return (Boolean) smState.get("autostart-enabled");               // been pung, return that value
        }
    }

    /**
     * <p>
     * Pingers may track when a service was last used.  If set to
     * non-zero this is the time and date of last use, converted to
     * milliseconds, as returned by System.getTimeMillis().  Its value is always 
     * set into the meta file for the pinger on each ping.
     * </p>
     *
     * <p>
     * Implementing ping/monitors may return a datestamp to indicate when the
     * service was last used by a job.
     * </p>
     *
     * @return A Long, representing the time of last known use of the service,
     *         as returned by System.getTimeMillis().
     */
    public long getLastUse()
    {
        return 0;
    }


    private String fmtArray(int[] array)
    {
        Object[] vals = new Object[array.length];
        StringBuffer sb = new StringBuffer();
        
        for ( int i = 0; i < array.length; i++ ) {
            sb.append("%3s ");
            vals[i] = Integer.toString(array[i]);
        }
        return String.format(sb.toString(), vals);
    }

    /**
     * <p>
     * This is used by the SM for running pingers internally as SM threads, to direct
     * the ping log into the SM log.
     *</p>
     *
     * <p>
     * External an custom pingers should generally not invoke this method unless the
     * intention is to fully manage their own logs.
     * </p>
     *
     * <p>
     * In all cases, the use of the {@link org.apache.uima.ducc.cli.AServicePing#doLog(String, Object...) }
     * method is strongly encouraged as it insures messages are logged into a
     * well-known and managed location.
     * </p>
     */
    public void setLogger(org.apache.uima.ducc.common.utils.DuccLogger logger)
    {
        this.duccLogger = logger;
    }

    /**
     * This is a convenience method for logging which enforces the use of the calling
     * method name and permits use of commas to separate fields in the message.  The
     * fields are converted via toString() and joined on a single space ' '. The composed
     * string is then written to the logger if it exists, and System.out otherwise.
     *
     * @param methodName This should be the named of the method calling doLog.
     * @param msg        This is a variable length parameter list which gets joined
     *                   on ' ' and emitted to the logger.
     */
    public void doLog(String methodName, Object ... msg)
    {        
        if ( !log_enabled ) return;

        StringBuffer buf = new StringBuffer(methodName);
        buf.append(" ");
        buf.append(Long.toString(service_id));
        for ( Object o : msg ) {
            buf.append(" ");
            if ( o == null ) {
                buf.append("<null>");
            } else {
                buf.append(o.toString());
            }
        }

        if ( duccLogger != null ) {
            duccLogger.info(methodName, null, buf);
        } else {
            System.out.println(buf);
        }
    }

    private void resetFailureWindow()
    {
        // This indicates an instance was restarted, which forces a cleaning of
        // failure conditions.
        total_failures = 0;
        failure_cursor = 0;
        for ( int i = 0; i < failure_window_size; i++ ) {
            failure_window[i] = 0;
        }
    }

    /**
     * <p>
     * This determines if there have been excessive service instance failures by tracking the 
     * number of failures, not consecutive, but rather within a window of time.  It may be
     * overridden by extending monitors.
     * </p>
     *
     * <p>
     * This default implementation uses a time window to determine if exessive failures
     * have occurred in a short period of time.  It operates off the two failure parameters
     * from the service registration:
     * <xmp>
     *     instance_failure_window  [time-in-minutes]
     *     instance_failure_limit   [number of failures]
     * </xmp>
     * </p>
     * <p>
     * If more than 'instance_failure_limit' failures occure within the preceding 
     * 'time-in-minutes' this method returns 'true' and the SM disables automatic
     * restart of instances.  Restart may be resumed by manually issuing a CLI start
     * to the service one the problem is resolved.
     * </p>
     *
     * <p>
     * Implementing ping/monitors may override this with custom logic to determine if a
     * service has had excessive failures.
     * </p>
     *
     * @return true if too many failures have been observed, false otherwise.  If 'true'
     * is returned, the SM no longer restarts failed instances.
     */
    public boolean isExcessiveFailures()
    {
        String methodName = "isExcessiveFailures";
        boolean excessive_failures = false;

        // Calculate total instance failures within some configured window.  If we get a cluster
        // of failures, signal excessive failures so SM stops spawning new ones.
        int failures = (Integer) smState.get("run-failures");
        doLog(methodName, "failures:", failures, "total_failures", total_failures);
        if ( failures > 0 ) {
            int diff = Math.max(0, failures - total_failures);  // nfailures since last update
            if ( diff < 0 ) {
                // This indicates an instance was restarted, which forces a cleaning of
                // failure conditions.  Here, it was restarted, and maybe yet another
                // error occurred.  To avoid complication let's just reset and let the
                // next ping actually do something about it.
                resetFailureWindow();
            } else if ( diff > 0 ) {
                total_failures += diff;
                failure_window[failure_cursor++] += diff;
            } else {
                failure_window[failure_cursor++] = 0;
            }
            failure_cursor = failure_cursor % failure_window_size;

            doLog(methodName, "failures", failures, "total_failures", total_failures, 
                  "failure_window", fmtArray(failure_window), "failure_cursor", failure_cursor);

            int windowed_failures = 0;
            excessive_failures = false;
            for ( int i = 0; i < failure_window_size; i++ ) {
                windowed_failures += failure_window[i];                    
            }
            if ( windowed_failures >= failure_max ) {
                excessive_failures = true;
            }
            doLog(methodName, "windowed_failures", windowed_failures, "excessive_failures", excessive_failures);
        } else if (total_failures > 0 ) {
            // we used to have failures bot not any more, something was restarted, let's reset the window
            resetFailureWindow();
        }
        return excessive_failures;
    }


}
