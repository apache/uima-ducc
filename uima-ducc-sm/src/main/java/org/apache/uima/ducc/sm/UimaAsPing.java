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
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

public class UimaAsPing
    extends AServicePing
{
    static final int QUEUE_SIZE_WINDOW = 3;
    int queue_threshold = 0;

    String ep;

    String endpoint;
    String broker;
    int    meta_timeout;

    String broker_host;
    int    broker_jmx_port;
    boolean connected;
    UimaAsServiceMonitor monitor;
    DuccLogger logger = null;

    int[] queueSizeWindow = new int[QUEUE_SIZE_WINDOW];
    int queueCursor = 0;

    UimaAsPing()
    {
        this.logger = null;
    }

    UimaAsPing(DuccLogger logger)
    {
        this.logger = logger;
    }

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

        queue_threshold = SystemPropertyResolver.getIntProperty("ducc.sm.max.queue.depth", 500);

        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        // there are a couple junky messages that slip by the above configurations.  turn the whole danged thing off.
        UIMAFramework.getLogger().setLevel(Level.OFF);

    }

    public void stop()
    {
        if ( monitor != null ) monitor.stop();
    }

    private void doLog(String methodName, String msg)
    {
        if ( logger == null ) {
            System.out.println(msg);
        } else {
            logger.info(methodName, null, msg);
        }
    }

    void evaluatePing(UimaAsServiceMonitor mon, ServiceStatistics stats)
    {
    	String methodName = "evaluatePing";
        try {
            mon.collect();

            if ( queue_threshold > 0 ) {         // only do this if a threshold is set
                // if the last 'n' q depths are > threshold, mark the service unhealthy
                // primitive, but maybe an OK first guess
                queueSizeWindow[queueCursor++ % QUEUE_SIZE_WINDOW] = (int)monitor.getQueueSize();
                int sum = 0;
                for ( int i = 0; i < QUEUE_SIZE_WINDOW; i++ ) {
                    sum += queueSizeWindow[i];
                }
                sum = sum / QUEUE_SIZE_WINDOW;
                stats.setHealthy( sum < queue_threshold ? true : false);
                // logger.debug(methodName, null, "EVAL: Q depth", monitor.getQueueSize(), "window", sum, "health", stats.isHealthy());
            } else {
                stats.setHealthy(true);
            }

            stats.setAlive(true);
            stats.setInfo(monitor.format());
        } catch ( Throwable t ) {
            stats.setAlive(false);
            stats.setHealthy(false);
            stats.setInfo(t.getMessage());
        }
    }

    public ServiceStatistics getStatistics()
    {
        String methodName = "getStatistics";
        ServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");

        // Instantiate Uima AS Client
        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
        Map<String, Object> appCtx = new HashMap<String, Object>();
        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
        appCtx.put(UimaAsynchronousEngine.Endpoint, endpoint);
        appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, meta_timeout);

        try {
            //	this sends GetMeta request and blocks waiting for a reply

            uimaAsEngine.initialize(appCtx);
            evaluatePing(monitor, statistics);       // if we get here, the get-meta worked well enough
            statistics.setAlive(true);

        } catch( ResourceInitializationException e) {
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

        return statistics;
    }

}
