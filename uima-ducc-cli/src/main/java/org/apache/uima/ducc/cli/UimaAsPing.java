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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

public class UimaAsPing
    extends AServicePing
{
    String ep;

    int failure_max = 5;            // max consecutive run failures before reporting excessive failures
                                    // which prevents restart of instances
    int current_failures = 0;       // current consecutive run failures
    int consecutive_failures = 0;   // n failures in consecutive pings
    int failure_window_size = 15;   // 15 minutes
    int monitor_rate = 1;           // ping rate, in minutes, min 1 used for calculations
    int fail_index = 0;
    int[] failure_window = null;    // tracks consecutive failures within a window
    int failure_cursor = 0;
    long service_id = 0;
    
    boolean excessive_failures = false;
    
    String endpoint;
    String broker;
    int    meta_timeout;

    String broker_host;
    int    broker_jmx_port;
    boolean connected;
    UimaAsServiceMonitor monitor;

    int[] queueSizeWindow;
    int queueCursor = 0;

    String nodeIp;
    String pid;
    boolean gmfail = false;
    boolean enable_log = false;
    
    public UimaAsPing()
    {
    }

    public void init(String args, String ep)
        throws Exception
    {
        this.ep = ep;

        // Ep is of the form UIMA-AS:queuename:broker
        int ndx = ep.indexOf(":");
        ep = ep.substring(ndx+1);
        ndx = ep.indexOf(":");
            
        this.endpoint = ep.substring(0, ndx).trim();
        this.broker = ep.substring(ndx+1).trim();

        // broker is a URL that we need to parse in order to get the actual host and port
        // for jmx
        URL url = null;
        try {                
            url = new URL(null, broker, new TcpStreamHandler());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid broker URL: " + broker);
        }
        broker_host = url.getHost();
        // not needed here fyi broker_port = url.getPort();

                
        UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        // there are a couple junky messages that slip by the above configurations.  turn the whole danged thing off.
        UIMAFramework.getLogger().setLevel(Level.INFO);

        if ( args == null ) {
            meta_timeout = 5000;
            broker_jmx_port = 1099;
        } else {
            // 'q_thresh=nn,window=mm,broker_jmx_port=1100,meta_timeout=10000'
            // turn the argument string into properties
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
            meta_timeout         = props.getIntProperty    ("meta-timeout"   , 5000);
            broker_jmx_port      = props.getIntProperty    ("broker-jmx-port", 1099);
            enable_log           = props.getBooleanProperty("enable-log"     , false);
            failure_max          = props.getIntProperty    ("max-failures"   , failure_max);
            failure_window_size  = props.getIntProperty    ("failure-window" , failure_window_size);
            failure_window = new int[failure_window_size];
            failure_cursor = 0;
        }

        doLog("<ctr>", null, "INIT: meta_timeout", meta_timeout, "broker-jmx-port", broker_jmx_port);

        this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
    }

    public void stop()
    {
        if ( monitor != null ) monitor.stop();
    }

    private void doLog(String methodName, Object ... msg)
    {
        if ( ! enable_log ) return;

        StringBuffer buf = new StringBuffer(methodName);
        for ( Object o : msg ) {
            buf.append(" ");
            if ( o == null ) {
                buf.append("<null>");
            } else {
                buf.append(o.toString());
            }
        }
        System.out.println(buf);
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

    void evaluateService(IServiceStatistics stats)
    {
    	String methodName = "evaluatePing";
        // Note that this particular pinger considers 'health' to be a function of whether
        // the get-mata worked AND the queue statistics.
        try {
            monitor.collect();
            stats.setHealthy(true);       // this pinger defines 'healthy' as
                                          // 'service responds to get-meta and broker returns jmx stats'


            monitor_rate = Integer.parseInt(smState.getProperty("monitor-rate") ) / 60000;       // convert to minutes
            service_id   = Long.parseLong(smState.getProperty("service-id"));            
            if (monitor_rate <= 0 ) monitor_rate = 1;                                            // minimum 1 minute allowed

            // Calculate total instance failures within some configured window.  If we get a cluster
            // of failures, signal excessive failures so SM stops spawning new ones.
            int failures = Integer.parseInt(smState.getProperty("run-failures"));
            doLog(methodName, "run-failures:", failures);
            if ( (failure_window != null) && (failures > 0) ) {
                int diff = failures - current_failures;  // nfailures since last update
                current_failures = failures;

                if ( diff > 0 ) {
                    failure_window[failure_cursor++] = diff;
                } else {
                    failure_window[failure_cursor++] = 0;                    
                }

                doLog(methodName, "failures", failures, "current_failures", current_failures, 
                      "failure_window", fmtArray(failure_window), "failure_cursor", failure_cursor);

                failure_cursor = failure_cursor % failure_window_size;



                int windowed_failures = 0;
                excessive_failures = false;
                for ( int i = 0; i < failure_window_size; i++ ) {
                    windowed_failures += failure_window[i];                    
                }
                if ( windowed_failures >= failure_max ) {
                    excessive_failures = true;
                }
                doLog(methodName, "windowed_failures", windowed_failures, "excessive_failures", excessive_failures);
            }

        } catch ( Throwable t ) {
            stats.setHealthy(false);
            monitor.setJmxFailure(t.getMessage());
        }
    }

    public boolean isExcessiveFailures()
    {
        return excessive_failures;
    }

    public IServiceStatistics getStatistics()
    {
        String methodName = "getStatistics";
        IServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");
        nodeIp = "N/A";
        pid = "N/A";

        evaluateService(statistics);       // if we get here, the get-meta worked well enough

        // Instantiate Uima AS Client
        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
        UimaCbListener listener = new UimaCbListener();
        uimaAsEngine.addStatusCallbackListener(listener);
        Map<String, Object> appCtx = new HashMap<String, Object>();
        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
        appCtx.put(UimaAsynchronousEngine.ENDPOINT, endpoint);
        appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, meta_timeout);

        try {
            uimaAsEngine.initialize(appCtx);
            statistics.setAlive(true);
            statistics.setHealthy(true && statistics.isHealthy());
            listener.ok();
        } catch( ResourceInitializationException e) {
            listener.timeout();
            doLog(methodName, "Cannot issue getMeta to: " + endpoint + ":" + broker);
            statistics.setHealthy(false);
            statistics.setAlive(false);
        } finally {
            try {
				uimaAsEngine.stop();
			} catch (Throwable e) {
				doLog(methodName, "Exception on UIMA-AS connection stop:" + e.toString());
			}
        }

        monitor.setSource(nodeIp, pid, gmfail);
        statistics.setInfo(monitor.format());

        return statistics;
    }

    class UimaCbListener extends UimaAsBaseCallbackListener 
    {
        public UimaCbListener()
        {
        }

        public void ok()
        {
            // String methodName = "UimaAsPing:get-meta";
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
            String methodName = "UimaAsPing:onBeforeProcessMeta";
            doLog(methodName, null, "Get-Meta received from ", IP, ":", p, "for", ep);
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
