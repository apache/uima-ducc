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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccProperties;

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

    private Connection connection;

    private Session producerSession;

    private MessageProducer producer;

    private Session consumerSession;

    private TemporaryQueue consumerDestination;

    private String brokerURI;

    private MessageConsumer consumer;

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
                doLog("init","Initializing UimaAsServiceMonitor: endpoint:"+endpoint+" broker_host:"+broker_host+" broker_jmx_port:"+broker_jmx_port);
                this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
            }
        }

    }

    private void initJMS(String brokerURI ) throws JMSException {
        String methodName = "initJMS";
    	ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
    	this.brokerURI = brokerURI; 
		connection = factory.createConnection();
        connection.start();
        doLog(methodName, "Connection started");

        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue producerQueue = producerSession.createQueue(endpoint);
        producer = producerSession.createProducer(producerQueue);
        consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerDestination = consumerSession.createTemporaryQueue();
        consumer = consumerSession.createConsumer(consumerDestination);
        doLog(methodName, "Created queues and sessions");
    }
    public void stop()
    { try {
        if (producerSession != null) {
            producerSession.close();
        }
        if (consumerSession != null) {
            consumerSession.close();
        }
        if ( connection != null ) {
        	connection.close();
        }
    } catch (JMSException e) {                                                                                                                                                                       
        e.printStackTrace();
    }

    	
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
            doLog("evaluateService", "EXCEPTION::::"+serializeThrowable(t));
        	stats.setHealthy(false);
            monitor.setJmxFailure(t.getMessage());
        }
    }

    private String serializeThrowable(Throwable t) {
    	String msg="";
    	if ( t != null ) {
    	   ByteArrayOutputStream bstream = new ByteArrayOutputStream();
    	   PrintStream pstream = new PrintStream(bstream);
    	   t.printStackTrace(pstream);
    	   pstream.close();
    	   msg = bstream.toString();
    	}
    	return msg;
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
        doLog(methodName, "***********************************************");

        IServiceStatistics statistics = new ServiceStatistics(false, false, "<NA>");
        String failure_reason = null;

        nodeIp = "N/A";
        pid = "N/A";

        evaluateService(statistics);       // if we get here, the get-meta worked well enough
        ExecutorService executor = null;
        Exception excp = null;
        gmfail = false;
        Future<Boolean> future = null;
        try {
        	initJMS(broker);
        	
            TextMessage msg = producerSession.createTextMessage();
            msg.setStringProperty(AsynchAEMessage.MessageFrom, consumerDestination.getQueueName());
            msg.setStringProperty(UIMAMessage.ServerURI, brokerURI);
            msg.setIntProperty(AsynchAEMessage.MessageType, AsynchAEMessage.Request);
            msg.setIntProperty(AsynchAEMessage.Command, AsynchAEMessage.GetMeta);
            msg.setJMSReplyTo(consumerDestination);
            msg.setText("");

            doLog(methodName, "Sending getMeta request to " + endpoint + " at " + brokerURI);
            producer.send(msg);
            long startTime = System.currentTimeMillis();
            executor = Executors.newSingleThreadExecutor();
            future = executor.submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                	// First receive() is to get IP and PID of the process that will process getMeta
                    ActiveMQTextMessage serviceInfoReply = (ActiveMQTextMessage) consumer.receive();
                    nodeIp = serviceInfoReply.getStringProperty(AsynchAEMessage.ServerIP); 
        		    pid = serviceInfoReply.getStringProperty(AsynchAEMessage.UimaASProcessPID);
                    // second receive() is for GetMeta reply
        		    // dont need to process the actual reply. If receive() succeeds we have 
        		    // a good GetMeta call
        		    consumer.receive();
                	return true;
                }
              }
            );
            // wait for getMeta reply and timeout if not received within allotted window
            future.get(meta_timeout, TimeUnit.MILLISECONDS);
            future.cancel(true);
            long replyTime = System.currentTimeMillis() - startTime;
            statistics.setAlive(true);
            statistics.setHealthy(true && statistics.isHealthy());

            statistics.setInfo("Get-meta took " + replyTime + " msecs.");
            doLog(methodName, "Reply received in ", replyTime, " ms");
            gmfail = false;
        } catch ( ExecutionException e) {
            excp = e;
            gmfail = true;
            statistics.setHealthy(false);
            statistics.setAlive(false);
            statistics.setInfo("Ping error: " + e);
            doLog(methodName, null, "Error while awaiting getmeta reply from ", nodeIp, "PID", pid);
        	if ( future != null ) {
        		future.cancel(true);
        	}
        } catch ( InterruptedException e) {
            excp = e;
            gmfail = true;
            statistics.setHealthy(false);
            statistics.setAlive(false);
            statistics.setInfo("Ping error: " + e);
            doLog(methodName, null, "Thread interrupted while waiting for getmeta reply from ", nodeIp, "PID", pid);
        	if ( future != null ) {
        		future.cancel(true);
        	}
        } catch( TimeoutException e) {
            excp = e;
            gmfail = true;
            statistics.setHealthy(false);
            statistics.setAlive(false);
            statistics.setInfo("Ping error: " + e);
            doLog(methodName, null, "Get-Meta timeout ("+meta_timeout+" ms) from ", nodeIp, "PID", pid);
        	if ( future != null ) {
        		future.cancel(true);
        	}
        	
        } catch (JMSException e) {
            excp = e;
            gmfail = true;
            statistics.setHealthy(false);
            statistics.setAlive(false);
            statistics.setInfo("Ping error: " + e);
            //e.printStackTrace();
        } finally {
        	stop();
        	if ( executor != null ) {
            	executor.shutdownNow();
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
 }
