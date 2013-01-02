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
{
    private String qname;
    private String broker_url;

    private QueueViewMBean monitoredQueue;
    private ServiceStatistics qstats;

    public UimaAsServiceMonitor(String qname, String broker_host, int broker_port)
    {
        this.qname = qname;
        this.broker_url = "service:jmx:rmi:///jndi/rmi://"+ broker_host + ":" + broker_port + "/jmxrmi";        
        this.qstats = new ServiceStatistics();
        // System.out.println("Broker url: " + broker_url);
    }

    public ServiceStatistics getServiceStatistics()
    {
    	return qstats;
    }
    
    /**
     * Connect to ActiveMq and find the mbean for the queue we're trying to monitor
     */
    public void connect()
        throws Exception
    {
        
        JMXServiceURL url = new JMXServiceURL(broker_url);
        JMXConnector jmxc = JMXConnectorFactory.connect(url);
        MBeanServerConnection conn = jmxc.getMBeanServerConnection();        
        String jmxDomain = "org.apache.activemq";  

        //
        // First get the broker name
        //
        ObjectName brokerObjectName = null;        
        for (Object nameObject : conn.queryNames(new ObjectName(jmxDomain + ":*,Type=Broker"), (QueryExp) null)) {
            //find the brokername object
            brokerObjectName = (ObjectName) nameObject;
            if (brokerObjectName.getCanonicalName().endsWith("Type=Broker")) {
                // Extract just the name from the canonical name
                String brokerName = brokerObjectName.getCanonicalName().substring(0, brokerObjectName.getCanonicalName().indexOf(","));
                //System.out.println("Canonical name of broker is " + brokerObjectName.getCanonicalName());
                //System.out.println("broker name is " + brokerName);
            }
        }

        //ObjectName activeMQ = new ObjectName("org.apache.activemq:BrokerName=" + broker_name +",Type=Broker");

        BrokerViewMBean mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, brokerObjectName ,BrokerViewMBean.class, true);
        for (ObjectName name : mbean.getQueues()) {
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

    public void collect()
        throws Throwable
    {
        qstats.setConsumerCount(monitoredQueue.getConsumerCount());
        qstats.setProducerCount(monitoredQueue.getProducerCount());
        qstats.setQueueSize(monitoredQueue.getQueueSize());
        qstats.setMinEnqueueTime(monitoredQueue.getMinEnqueueTime());
        qstats.setMaxEnqueueTime(monitoredQueue.getMaxEnqueueTime());
        qstats.setInFlightCount(monitoredQueue.getInFlightCount());
        qstats.setDequeueCount(monitoredQueue.getDequeueCount());
        qstats.setEnqueueCount(monitoredQueue.getEnqueueCount());
        qstats.setDispatchCount(monitoredQueue.getDispatchCount());
        qstats.setExpiredCount(monitoredQueue.getExpiredCount());
    }

    public static void main(String[] args)
    {
        // System.out.println(args[0] + " " + args[1] + " " + args[2]);
        UimaAsServiceMonitor m = new UimaAsServiceMonitor(args[0], args[1], Integer.parseInt(args[2])); // qname, broker host, broker port
        ServiceStatistics qs = m.qstats;

        try {
			m.connect();                      // connect to amq
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.out.println("Cannot connect:");
			e1.printStackTrace();
            return;
		}

        int lim = 10;
        int ctr =   0;
        while ( true ) {

            try {
                m.collect();
            } catch (Throwable t) {
                System.out.println("Cannot collect stats.  The queue may have been deleted. Details:");
                t.printStackTrace();
                return;
            }
            if ( (ctr++) % lim == 0 ) {
                System.out.println(qs.header());
            }
            System.out.println(qs.toString());
            try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
        }
    }
}
