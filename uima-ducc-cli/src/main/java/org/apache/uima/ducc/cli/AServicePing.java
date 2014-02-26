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
    protected int total_failures = 0;           // current total run failures. usually monotonically increasing.
    protected int failure_max = 3;              // max allowed failures within any given window

    protected int failure_window_period = 30;   // 30 minutes. overridden at first ping
    protected int failure_window_size = failure_window_period;  // assume 1 ping per minute
    protected int monitor_rate = 1;             // ping rate, in minutes, min 1 used for calculations

    protected boolean log_enabled = false;
    protected long service_id = 0;    

    protected Map<String, Object> smState;
    protected Map<String, Object> initializationState;

    protected org.apache.uima.ducc.common.utils.DuccLogger duccLogger = 
        org.apache.uima.ducc.common.utils.DuccLogger.getLogger(this.getClass().getName(), "PING");	

    /**
     * Called by the ping driver, to pass in useful things the pinger may want.
     * @param arguments This is passed in from the service specification's
     *                  service_ping_arguments string.
     *
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     */
    public abstract void init(String arguments, String endpoint) throws Exception;

    /**
     * Called by the ping driver to initialize static information about the service and
     * pinger.  This guy calls the public init() method and is not intended for public
     * consumption.
     *
     * This guy initializes the default failure monitor, service id, whether the
     * service log is enabled, and the monitor (ping) rate, all of which are
     * free to be used by derived classes.
     *
     * @param arguments This is passed in from the service specification's
     *                  service_ping_arguments string.
     *
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     *
     * @param initProps Properties file with static data about the service and 
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
     * @return This object contains the informaton the service manager and web server require
     *     for correct management and display of the service.
     */
    public abstract IServiceStatistics getStatistics();

    /**
     * Current state of the monitored service is passed in here.
     */    
    public void setSmState(Map<String, Object> props)
    {
        smState = props;
    }

    /**
     * Getter of the service state;  Implementors may just access it directly if they want.
     */
    public Map<String, Object> getSmState() 
    {
        return smState;
    }

    /**
     * Called by the service manager to query the number of additional needed service instances.
     */
    public int getAdditions()
    {
        return 0;
    }

    /**
     * Called by the service manager to query the number of service insances to dump.
     */
    public Long[] getDeletions()
    {
        return null;
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
     * This is intended for use by the SM when it drives a pinger in an internal thread.  External
     * pingers won't have this set.  
     *
     * However, external pinger's stdout is picked up by DUCC so the logger will still print
     * stuff to the service log without the use of the ducc logger.
     */
    public void setLogger(org.apache.uima.ducc.common.utils.DuccLogger logger)
    {
        this.duccLogger = logger;
    }

    private void doLog(String methodName, Object ... msg)
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
     * This determines if there have been excessive service instance failures by tracking the 
     * number of failures, not consecutive, but rather within a window of time.  It may be
     * overridden by extending monitors.
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
