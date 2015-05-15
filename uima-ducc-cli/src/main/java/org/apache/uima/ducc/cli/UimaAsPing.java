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
        UIMAFramework.getLogger().setLevel(Level.OFF);

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
            meta_timeout          = props.getIntProperty    ("meta-timeout"   , 5000);
            String broker_tmp_jmx = props.getProperty       ("broker-jmx-port");
            if ( broker_tmp_jmx.equals("none") ) {
                broker_jmx_port = -1;
                this.monitor = null;
            } else {
                broker_jmx_port = props.getIntProperty("broker-jmx-port", 1099);
                this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
            }
        }

    }

    public void stop()
    {
        if ( monitor != null ) monitor.stop();
    }

    void evaluateService(IServiceStatistics stats)
    {
    	//String methodName = "evaluatePing";
        // Note that this particular pinger considers 'health' to be a function of whether
        // the get-mata worked AND the queue statistics.
        try {
            if ( monitor != null ) {
                monitor.collect();
                long cc = monitor.getProducerCount();
                if ( cc > 0 ) {
                    last_use = System.currentTimeMillis();
                }                
            }
            stats.setHealthy(true);       // this pinger defines 'healthy' as
                                          // 'service responds to get-meta and broker returns jmx stats'
        } catch ( Throwable t ) {
            stats.setHealthy(false);
            monitor.setJmxFailure(t.getMessage());
        }
    }

    /**
     * Override from AServicePing
     */
    public long getLastUse()
    {
        return last_use;
    }

    public IServiceStatistics getStatistics()
    {
        String methodName = "getStatistics";
        IServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");
        String failure_reason = null;

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
        appCtx.put(UIMAFramework.CAS_INITIAL_HEAP_SIZE, 1000);

        ResourceInitializationException excp = null;
        gmfail = false;
        try {
            uimaAsEngine.initialize(appCtx);
            statistics.setAlive(true);
            statistics.setHealthy(true && statistics.isHealthy());
            listener.ok();
        } catch( ResourceInitializationException e ) {
            excp = e;
            listener.timeout();
            statistics.setHealthy(false);
            statistics.setAlive(false);
        } finally {
            try {
				uimaAsEngine.stop();
			} catch (Throwable e) {
				doLog(methodName, "Exception on UIMA-AS connection stop: " + e.toString());
			}
        }

        if ( gmfail || excp != null ) {
            failure_reason = "Cannot issue getMeta to: " + endpoint + ":" + broker; 
            if ( excp != null ) {
                if (excp.getCause() == null ) {
                    failure_reason = failure_reason + ": " + excp.toString();
                } else {
                    failure_reason = failure_reason + ": " + excp.getCause();
                }
            }
            doLog(methodName, failure_reason);
        }

        if ( monitor == null ) {                   // no jmx active
            if ( failure_reason != null ) {
                statistics.setInfo(failure_reason);
            } else {
                statistics.setInfo("Ping to " + nodeIp + ": " + pid + " ok. (JMX disabled.)");
            }
        } else {
            monitor.setSource(nodeIp, pid, gmfail, failure_reason);
            statistics.setInfo(monitor.format());
        }

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
