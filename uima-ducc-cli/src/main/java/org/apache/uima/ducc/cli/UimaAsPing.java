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
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

// 'q_thresh=nn,window=mm,broker_jmx=1100,meta_timeout=10000'
public class UimaAsPing
    extends AServicePing
{
    int window = 3;
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

    int[] queueSizeWindow;
    int queueCursor = 0;

    String nodeIp;
    String pid;
    boolean gmfail = false;

    public UimaAsPing()
    {
        this.logger = null;
    }

    public UimaAsPing(DuccLogger logger)
    {
        this.logger = logger;
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

                
        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        // there are a couple junky messages that slip by the above configurations.  turn the whole danged thing off.
        UIMAFramework.getLogger().setLevel(Level.INFO);

        if ( args == null ) {
            meta_timeout = 5000;
            broker_jmx_port = 1099;
            queue_threshold = 0;
            window = 3;
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
            meta_timeout = props.getIntProperty("meta_timeout", 5000);
            broker_jmx_port = props.getIntProperty("broker_jmx_port", 1099);
            queue_threshold = props.getIntProperty("queue_threshold", 0);
            window = props.getIntProperty("window", 3);
        }
        queueSizeWindow = new int[window];
        logger.debug("<ctr>", null, "INIT: meta_timeout", meta_timeout, "broker_jmx_port", broker_jmx_port, "queue_threshold", queue_threshold, "window", window);

        this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
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

    void evaluateBrokerStatistics(IServiceStatistics stats)
    {
    	String methodName = "evaluatePing";
        // Note that this particular pinger considers 'health' to be a function of whether
        // the get-mata worked AND the queue statistics.
        try {
            monitor.collect();

            if ( queue_threshold > 0 ) {         // only do this if a threshold is set
                // if the last 'n' q depths are > threshold, mark the service unhealthy
                // primitive, but maybe an OK first guess
                queueSizeWindow[queueCursor++ % window] = (int)monitor.getQueueSize();
                int sum = 0;
                for ( int i = 0; i < window; i++ ) {
                    sum += queueSizeWindow[i];
                }
                sum = sum / window;
                stats.setHealthy( sum < queue_threshold ? true : false );
                logger.debug(methodName, null, "EVAL: Q depth", monitor.getQueueSize(), "window", sum, "health", stats.isHealthy());
            } else {
                stats.setHealthy(true);
            }

        } catch ( Throwable t ) {
            stats.setHealthy(false);
            monitor.setJmxFailure(t.getMessage());
        }
    }

    public IServiceStatistics getStatistics()
    {
        String methodName = "getStatistics";
        IServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");
        nodeIp = "N/A";
        pid = "N/A";

        evaluateBrokerStatistics(statistics);       // if we get here, the get-meta worked well enough

        // Instantiate Uima AS Client
        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
        UimaCbListener listener = new UimaCbListener();
        uimaAsEngine.addStatusCallbackListener(listener);
        Map<String, Object> appCtx = new HashMap<String, Object>();
        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
        appCtx.put(UimaAsynchronousEngine.Endpoint, endpoint);
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
            // logger.info(methodName, null, "Get-Meta received from ", nodeIp, "PID", pid);
            gmfail = false;
        }

        public void timeout()
        {
            String methodName = "UimaAsPing:get-meta";
            logger.info(methodName, null, "Get-Meta timeout from ", nodeIp, "PID", pid);
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
            logger.info(methodName, null, "Get-Meta received from ", IP, ":", p, "for", ep);
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
