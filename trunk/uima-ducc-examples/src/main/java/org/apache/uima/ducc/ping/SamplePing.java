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
package org.apache.uima.ducc.ping;

/*
 * IMPORTANT: This is a sample custom pinger for illustration purposes only.  It is not
 *            supported in any way.
 */

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngineCommon_impl;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.ducc.cli.AServicePing;
import org.apache.uima.ducc.cli.ServiceStatistics;
import org.apache.uima.ducc.cli.UimaAsServiceMonitor;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;



// 'q_thresh=nn,window=mm,broker_jmx=1100,meta_timeout=10000'
public class SamplePing
    extends AServicePing
{
    String ep;                           // Endpoint, passed in during initialization

    String qname;                        // Service queue name, parsed from ep
    String broker;                       // Service broker, parsed from ep
    int    meta_timeout = 5000;          // default

    String broker_host;                  // AMQ hostname, parsed from 'broker' URL
    int    broker_jmx_port = 1099;       // AMQ jmx port, default

    UimaAsServiceMonitor monitor;        // Part of ping API, knows how to query AMQ for broker stats

    String nodeIp;                       // For UIMA-AS get-meta callback, IP of node answering get-meta
    String pid;                          //  "     "       "        "    , process answering get-meta
    boolean gmfail = false;              // Did get=meta work?
    boolean fast_shrink = true;          // If false, don't shrink instances if there are producers
                                         // still connected to the Q.

    int additions = 0;                   // number of additions to signal to SM
    Long[] deletions = null;             // which specific instances to shrink

    int min_instances = 0;               // minimum instances to maintain
    int max_instances = 10;              // max instances to allow

    int max_growth = 5;                  // max processes to grow at any time
    double goal = 2.00;                  // want to get enqueue time to within this factor of individual service time

    int cursor = 0;                      // growth and shrinkage window cursor
    int expansion_period = 5;            // size of window in minutes
    int window_size = expansion_period;  // size of window in pings (panes)
    int[] expansion_window;              // expansion window
    int[] deletion_window;               // shrinkage window

    public SamplePing()
    {

    }

    public void init(String args, String ep)
        throws Exception
    {
        String methodName = "init";

        this.ep = ep;
        doLog(methodName, "Ping.init(" + args + ", " + ep + " start.");

        // Ep is of the form UIMA-AS:queuename:broker. Parse out queue name and broker URL
        int ndx = ep.indexOf(":");
        ep = ep.substring(ndx+1);
        ndx = ep.indexOf(":");
            
        this.qname = ep.substring(0, ndx).trim();
        this.broker = ep.substring(ndx+1).trim();

        // broker is a URL that we need to parse in order to get the actual host for jmx calls
        URL url = null;
        try {                
            url = new URL(null, broker, new TcpStreamHandler());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid broker URL: " + broker);
        }
        broker_host = url.getHost();
        
        
        // inhibit noisy default UIMAAs logging
        UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        UIMAFramework.getLogger().setLevel(Level.INFO);

        // Parse out the pinger arguments which are comma-delimeted in the argument string
        // We do this by splitting on ',', then writing as strings to a StringReader, and
        // finally loading a PropertiesFile from them.  DuccProperties is used because it
        // smart typed extraction of the properties.
        if ( args != null ) {

            String[] as = args.split(",");
            StringWriter sw = new StringWriter();
            for ( String s : as ) sw.write(s + "\n");
            StringReader sr = new StringReader(sw.toString());            
            DuccProperties props = new DuccProperties();
            try {
                props.load(sr);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            meta_timeout = props.getIntProperty("meta-timeout", meta_timeout);
            broker_jmx_port = props.getIntProperty("broker-jmx-port", broker_jmx_port);
            expansion_period = props.getIntProperty("window", expansion_period);
            min_instances = props.getIntProperty("min", min_instances);
            max_instances = props.getIntProperty("max", max_instances);
            max_growth = props.getIntProperty("max-growth", max_growth);
            fast_shrink = props.getBooleanProperty("fast-shrink", true);
            goal = props.getDoubleProperty("goal", goal);
        }

        // Set up expansion/deletion windows with window size always 1 minute regardless
        // of ping frequency.
        double calls_per_minute = 60000.00 / monitor_rate;
        window_size = (int) ( ((double)expansion_period) * calls_per_minute);

        expansion_window = new int[window_size];
        deletion_window  = new int[window_size];

        doLog("<ctr>", 
              "INIT: meta-timeout", meta_timeout, 
              "broker-host", broker_host,
              "broker-jmx-port", broker_jmx_port, 
              "monitor-window", expansion_period,
              "window-size", window_size,
              "monitor rate", monitor_rate,
              "max-instances", max_instances,
              "min-instances", min_instances,
              "max-growth", max_growth,
              "goal", goal,
              "fast-shrink", fast_shrink);

        // Set up the jmx queue monitor and reset statistics to insure each calculation is
        // based on current data, not old data.
        this.monitor = new UimaAsServiceMonitor(qname, broker_host, broker_jmx_port);
        monitor.resetStatistics();
    }

    // Release resources when terminated.
    public void stop()
    {
    	String methodName = "stop";
        if ( monitor != null ) monitor.stop();
        doLog(methodName, "------------ Stop signal arrives, pinger exits.");
    }

    // This is called by the Service Manager to collect the pinger data.  This pinger 
    // issues a get-meta to the service to insure it's alive, then collects queue statistics via JMX
    // and determines expansion and deletion for return when SM asks about them.
    public IServiceStatistics getStatistics()
    {
        String methodName = "getStatistics";
        IServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");
        nodeIp = "N/A";
        pid = "N/A";

        // "health" has no real meaning.  maybe we can get rid of it one day?
        try {
            monitor.collect();                         // Get jmx stuff
            statistics.setHealthy(true);
        } catch ( Throwable t ) {
            statistics.setHealthy(false);
            monitor.setJmxFailure(t.getMessage());
        }

        // Instantiate Uima AS Client and issue get-meta
        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
        UimaCbListener listener = new UimaCbListener();
        uimaAsEngine.addStatusCallbackListener(listener);
        Map<String, Object> appCtx = new HashMap<String, Object>();
        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
        appCtx.put(UimaAsynchronousEngine.ENDPOINT, qname);
        appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, meta_timeout);

        try {
            uimaAsEngine.initialize(appCtx);                     // This performs the get-meta
            statistics.setAlive(true);
            statistics.setHealthy(true && statistics.isHealthy());
            listener.ok();
        } catch( ResourceInitializationException e) {
            listener.timeout();                                 // Service not responding
            doLog(methodName, "Cannot issue getMeta to: " + qname + ":" + broker);
            statistics.setHealthy(false);
            statistics.setAlive(false);
        } finally {
            try {
				uimaAsEngine.stop();
			} catch (Throwable e) {
				doLog(methodName, "Exception on UIMA-AS connection stop:" + e.toString());
			}
        }

        monitor.setSource(nodeIp, pid, gmfail);                  // remember who responded
        statistics.setInfo(monitor.format());                    // set string for web server

        calculateNewDeployment(statistics);                      // Decide on instance expansion or deletion

        return statistics;
    }

    /**
     * Override from AServicePing, set in calculateNewDeployment while analyzing queue statistics
     */
    public long getLastUse()
    {
        return last_use;                 
    }

    // ================================================================================
    //                                   Service Mini-scheduler
    // ================================================================================
    void calculateNewDeployment(IServiceStatistics stats)
    {
        String methodName = "calculateNewDeployment";
        // Rate per overall service
        //    Rs = Qt / Qd
        // Per-instance response
        //    Ri = Rs * ninst

        // This scheduler is going to chase Ri, attempting to keep Qt <= 4 seconds
        double eT     = monitor.getEnqueueTime();  // average time things stay in queue
        long   Q      = monitor.getQueueSize();    // current queue size (depth)
        long   cc     = monitor.getConsumerCount();
        long   pc     = monitor.getProducerCount();

        double Ti;                                 // service time per instance (process)
        double Tt;                                 // service time per thread

        Long[] instances     = (Long[]) smState.get("all-instances");
        int    ninst         =  instances.length;
        Long[] act_instances = (Long[]) smState.get("active-instances");
        int    active        = act_instances.length;

        if ( pc > 0 ) {
            last_use = System.currentTimeMillis();
        }

        //
        // Must calculate number of threads per instance up front in order to properly
        //   count ninstances later as it changes. 
        //
        // let cc = number of comsumers
        //     ni = number of instances
        //     nt = threads per instance
        // then
        //     cc = ni * nt + ni
        // and
        //     nt = cc / ni - 1
        //
        // Then later, knowing cc, we calculate ni:
        //     ni = cc / (nt + 1)
        //
        int nthreads = (int) (cc / ninst) - 1;

        doLog(methodName, stats.getInfo());

        additions = 0;
        deletions = null;

        int new_ni = 0;
        // what we're looking for it Tt: the average time it takes on thread to compute one unit of work.
        if ( Q == 0 ) {
            if ( (pc != 0) && ( !fast_shrink) ) {
                doLog(methodName, "Inhibit shrinkage because pc =", pc, "Q =", Q);
                deletion_window[cursor] = 0;
            } else {
                deletion_window[cursor] = 1;
            }

            expansion_window[cursor] = 0;         // no expansion if no queue
        } else {

            Ti = (eT / Q) * active;  
            Tt = Ti * nthreads;

            // we want to try to keep the eT at some factor of the Tt.  This stuff varies linearly so it's 
            // relatively easy.  If eTd is desired eT, and the goal is g:
            double g = Tt * goal;   // we want to get eT close to this
            double r = eT / g;      // the ratio of current queue to to desired, which is the amount we need to 
                                    // change the instances by
            doLog(methodName, "eT", eT, "Q", Q, "cc", cc, "Ti", Ti, "Tt", Tt, "g", g, "r", r, 
                  "active", active, "ninstances", ninst, "max_instances", max_instances);

            if ( r > 1 ) {          // we want moref

                // We must smooth it out.  First get the delta instances.
                // Then cap it accounting for instances started but not yet connected to the q
                // Then cap it on the max for the service.
                // Then cap it on (arbitrarily) 5 so we don't blast too many new instances at once
                new_ni = (int) Math.ceil(active * r);  // delta
                new_ni = Math.max(new_ni - ninst, 0);  // here we account for intances that aren't yet started
                new_ni = Math.min(new_ni, max_instances); // cap on configured max
                new_ni = Math.min(new_ni, max_growth);    // cap on growth rate


                if ( new_ni > 0 ) {                         // do we actually expand in the end?
                    doLog(methodName, "Expand, new_n1:", new_ni);
                    expansion_window[cursor] = 1;
                } else {
                    doLog(methodName, "Don't expand, new_n1:", new_ni);
                    expansion_window[cursor] = 0;
                }
            } else {
                doLog(methodName, "Don't expand, r < 1.0:", r);
                expansion_window[cursor] = 0;
            }

            if ( r < .5 ) {                            // we're over-provisioned to the goal
                if ( (pc != 0) && (!fast_shrink) ) {
                    // never shrink if there are producers
                    doLog(methodName, "Inhibit shrinkage because pc =", pc, "r=", r);
                    deletion_window[cursor] = 0;
                } else {
                    doLog(methodName, "Allow shrinkage: r =", r, "pc =", pc);
                    deletion_window[cursor] = 1;           // if we stay that way we'll try dropping one instance
                }
            } else {
                deletion_window[cursor] = 0;          // don't delete if there's a queue
            }
        }

        doLog(methodName, "Expansion window:", Arrays.toString(expansion_window));
        doLog(methodName, "Deletion  window:", Arrays.toString(deletion_window));
        // more smoothing, only expand every few (expansion_period) instances, and only if we've been needing more
        // for at while
        int etot = 0;
        int dtot = 0;
        for (int i = 0; i < window_size; i++ ) {
            etot += expansion_window[i];
            dtot += deletion_window[i];            
        }

        if ( (etot == expansion_window.length) && (new_ni > 0) ) {
            additions = new_ni;;
            deletions = null;
            if ( ninst > active ) {         
                additions = 0;      // don't expand if we're still waiting for these to come alive
            } else {
                expansion_window[cursor] = 0;       // if we expand we use one slot in order to govern expansion  
            }
        }

        if ( dtot == deletion_window.length ) {
            additions = 0;
            if ( ninst > min_instances ) {
                deletions = new Long[1];
                deletions[0] = act_instances[act_instances.length - 1];
                doLog(methodName, "Deletions:", deletions[0]);
                deletion_window[cursor] = 0;     // if we shrink we lose one slot to govern shrinkage
            } 
        }

        doLog(methodName, "Cursor before:", cursor, "window_size", window_size);
        cursor = ++cursor % window_size;
        doLog(methodName, "Cursor after:", cursor);

    }

    // ================================================================================
    //                                         NEW INTERFACES
    // ================================================================================
    /**
     * Implement this to indicate how many new instances to start.  The value here
     * is calculted above in calculateNewDeployment.
     */
    public int getAdditions()
    {
        return additions;
    }

    /**
     * Implement this to indicate how many instances to stop.  The value here
     * is calculted above in calculateNewDeployment.
     */
    public Long[] getDeletions()
    {
        return deletions;
    }

    // ================================================================================
    //                                         END NEW INTERFACES
    // ================================================================================

    /**
     * This is a callback class for the UIMA-AS get-meta, that tells us which process on 
     * which host responded to the get-meta.
     */
    class UimaCbListener extends UimaAsBaseCallbackListener 
    {
        public UimaCbListener()
        {
        }

        public void ok()
        {
            // String methodName = "UimaAsPing:get-meta";
            // logger.info(methodName, null, "Get-Meta received from ", nodeIp, "PID", pid);
            gmfail = false;
        }

        public void timeout()
        {
            String methodName = "UimaAsPing:get-meta";
            doLog(methodName, null, "Get-Meta timeout from ", nodeIp, "PID", pid);
            gmfail = true;
        }

        public void onBeforeMessageSend(UimaASProcessStatus status) 
        {
        }
	
//        private void onBeforeMessageSendHandler(UimaASProcessStatus status) 
//        {
//        }
	
        public void onBeforeProcessMeta(String IP, String p)
        {
            //String methodName = "UimaAsPing:onBeforeProcessMeta";
            //doLog(methodName, null, "Get-Meta received from ", IP, ":", p, "for", ep);
            pid = p;
            nodeIp = IP;
        }
	
//        private void onBeforeProcessCASHandler(UimaASProcessStatus status, String nodeIP, String pid) 
//        {
//        }
	
        public void initializationComplete(EntityProcessStatus aStatus) 
        {
        }

        public void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) 
        {
        }

        public void collectionProcessComplete(EntityProcessStatus aStatus) 
        {
        }
    }

}
