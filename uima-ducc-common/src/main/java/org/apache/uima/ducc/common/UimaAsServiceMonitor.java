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
package org.apache.uima.ducc.common;

import java.io.IOException;
import java.text.DecimalFormat;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

public class UimaAsServiceMonitor
    extends AServicePing
{

    private String qname;
    private String broker_url;

    private JMXConnector jmxc;
    BrokerViewMBean brokerMBean;
    private QueueViewMBean monitoredQueue;
    private ServiceStatistics qstats;

    double enqueueTime ; 
    long consumerCount ;
    long producerCount ;
    long queueSize     ;
    long minEnqueueTime;
    long maxEnqueueTime;
    long inFlightCount ;
    long dequeueCount  ;
    long enqueueCount  ;
    long dispatchCount ;
    long expiredCount  ;
    
    boolean alive = false;
    boolean healthy = false;



    public UimaAsServiceMonitor(String qname, String broker_host, int broker_port)
    {
        this.qname = qname;
        this.broker_url = "service:jmx:rmi:///jndi/rmi://"+ broker_host + ":" + broker_port + "/jmxrmi";        
        this.qstats = new ServiceStatistics(false, false, "N/A");
        // System.out.println("Broker url: " + broker_url);

    }

    public ServiceStatistics getStatistics()
    {
        try {
            collect();
            qstats.setAlive(true);        // if we don't croak gathering stuff, we're not dead
            qstats.setHealthy(true);
            qstats.setInfo(format());
            qstats.setErrorString(null);
        } catch ( Throwable t ) {
            qstats.setAlive(false);        // if we don't croak gathering stuff, we're not dead
            qstats.setHealthy(false);
            qstats.setErrorString(t.getMessage());
        }
    	return qstats;
    }
    
    /**
     * Connect to ActiveMq and find the mbean for the queue we're trying to monitor
     */
    public void init(String parm /* parm not used in this impl */)
        throws Exception
    {
        // String methodName = "init";

        JMXServiceURL url = new JMXServiceURL(broker_url);
        jmxc = JMXConnectorFactory.connect(url);
        MBeanServerConnection conn = jmxc.getMBeanServerConnection();        
        String jmxDomain = "org.apache.activemq";  

        //
        // First get the broker name
        //
        ObjectName brokerObjectName = null;        
        for (Object nameObject : conn.queryNames(new ObjectName(jmxDomain + ":*,Type=Broker"), (QueryExp) null)) {
            //find the brokername object
            brokerObjectName = (ObjectName) nameObject;
            //if (brokerObjectName.getCanonicalName().endsWith("Type=Broker")) {
                // Extract just the name from the canonical name
                //String brokerName = brokerObjectName.getCanonicalName().substring(0, brokerObjectName.getCanonicalName().indexOf(","));
                //System.out.println("Canonical name of broker is " + brokerObjectName.getCanonicalName());
                //System.out.println("broker name is " + brokerName);
            //}
        }

        //ObjectName activeMQ = new ObjectName("org.apache.activemq:BrokerName=" + broker_name +",Type=Broker");

        brokerMBean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, brokerObjectName ,BrokerViewMBean.class, true);
        for (ObjectName name : brokerMBean.getQueues()) {
            QueueViewMBean qView = (QueueViewMBean)
                MBeanServerInvocationHandler.newProxyInstance(conn, name, QueueViewMBean.class, true);
            
            if ( qname.equals(qView.getName()) ) {
                monitoredQueue = qView;
            } else {
                // System.out.println("Skipping queue " + qView.getName());
            }
        }
        
        if ( monitoredQueue == null ) {
            throw new IllegalStateException("Cannot find queue: " + qname);
        }        
    }

    public void clearQueues()
        throws Throwable
    {
        // String methodName = "clearQueues";
        init(null);

        if ( ( qname != null ) && ( brokerMBean != null ) ) {
            brokerMBean.removeQueue(qname);
        }
        stop();
    }

    public void stop()
    {
        try {
			if ( jmxc != null ) {
			    jmxc.close();
			    jmxc = null;
			}
		} catch (IOException e) {
			// don't really care
		}
    }

    private String format()
    {
        return "AveNQ[" + new DecimalFormat("####.##").format(enqueueTime)
            +  "] Consum[" + consumerCount
            +  "] Prod[" + producerCount
            +  "] Qsize[" + queueSize
            +  "] minNQ[" + minEnqueueTime
            +  "] maxNQ[" + maxEnqueueTime
            +  "] expCnt[" + expiredCount
            +  "] inFlt[" + inFlightCount
            +  "] DQ[" + dequeueCount
            +  "] NQ[" + enqueueCount
            +  "] NDisp[" + dispatchCount
            + "]"
            ;

    }

    private void collect()
        throws Throwable
    {
    	// String methodName = "collect";
        init(null);
        if ( monitoredQueue != null ) {
            enqueueTime    = monitoredQueue.getAverageEnqueueTime();
            consumerCount  = monitoredQueue.getConsumerCount();
            producerCount  = monitoredQueue.getProducerCount();
            queueSize      = monitoredQueue.getQueueSize();
            minEnqueueTime = monitoredQueue.getMinEnqueueTime();
            maxEnqueueTime = monitoredQueue.getMaxEnqueueTime();
            inFlightCount  = monitoredQueue.getInFlightCount();
            dequeueCount   = monitoredQueue.getDequeueCount();
            enqueueCount   = monitoredQueue.getEnqueueCount();
            dispatchCount  = monitoredQueue.getDispatchCount();
            expiredCount   = monitoredQueue.getExpiredCount();
        } else {
            enqueueTime    = 0;
            consumerCount  = 0;
            producerCount  = 0;
            queueSize      = 0;
            minEnqueueTime = 0;
            maxEnqueueTime = 0;
            inFlightCount  = 0;
            dequeueCount   = 0;
            enqueueCount   = 0;
            dispatchCount  = 0;
            expiredCount   = 0;
        }

        stop();
    }

    public static void main(String[] args)
    {
        // System.out.println(args[0] + " " + args[1] + " " + args[2]);
        UimaAsServiceMonitor m = new UimaAsServiceMonitor(args[0], args[1], Integer.parseInt(args[2])); // qname, broker host, broker port

        try {
			m.init(null);                      // connect to amq
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.out.println("Cannot connect:");
			e1.printStackTrace();
            return;
		}


        while ( true ) {
            ServiceStatistics qs = null;
            try {
                qs = m.getStatistics();
            } catch (Throwable t) {
                System.out.println("Cannot collect stats.  The queue may have been deleted. Details:");
                t.printStackTrace();
                return;
            }
            System.out.println(qs.toString());
            try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
        }
    }
}
