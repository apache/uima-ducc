package org.apache.uima.ducc.sm;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.ducc.common.AServicePing;
import org.apache.uima.ducc.common.ServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.UimaAsServiceMonitor;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
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

    public void init(String ep)
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

        String to = System.getProperty("ducc.sm.meta.ping.timeout");
        meta_timeout = Integer.parseInt(to);

        
        broker_jmx_port = SystemPropertyResolver.getIntProperty("ducc.sm.meta.jmx.port", 1099);
        this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
        init_monitor();

        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        // there are a couple junky messages that slip by the above configurations.  turn the whole danged thing off.
        UIMAFramework.getLogger().setLevel(Level.OFF);

    }

    public void stop()
    {
    }

    private synchronized void init_monitor()
    {
        if ( ! connected ) {
            try {
                System.out.println("Initializing monitor");
                monitor.init(ep);
                connected = true;
                System.out.println("Monitor initialized");
            } catch (Throwable t ) {
                connected = false;
                // t.printStackTrace();
                System.err.println("Cannot initialize monitor: " + t.getMessage());
            }
        }
    }

    public ServiceStatistics getStatistics()
    {
        ServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");

        // Instantiate Uima AS Client
        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
        Map<String, Object> appCtx = new HashMap<String, Object>();
        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
        appCtx.put(UimaAsynchronousEngine.Endpoint, endpoint);
        appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, meta_timeout);

        try {
            //	this sends GetMeta request and blocks waiting for a reply
            init_monitor();
            if ( connected ) {
                statistics = monitor.getStatistics();
            } else {
                return statistics;
            }

            uimaAsEngine.initialize(appCtx);
            statistics.setAlive(true);
            statistics.setHealthy(true);
            // System.out.println("getMeta ok: " + ep);

        } catch( ResourceInitializationException e) {
            System.out.println("Cannot issue getMeta: " + e.getMessage());
            e.printStackTrace();
        } finally {
            uimaAsEngine.stop();
        }

        return statistics;
    }

}
