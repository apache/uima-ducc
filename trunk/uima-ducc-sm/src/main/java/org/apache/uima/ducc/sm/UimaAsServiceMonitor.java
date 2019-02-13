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
package org.apache.uima.ducc.sm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.uima.ducc.cli.ServiceStatistics;
import org.apache.uima.ducc.common.IServiceStatistics;

public class UimaAsServiceMonitor
{

    private String qname;
    private String broker_url;

    private JMXConnector jmxc;
    private BrokerViewMBean brokerMBean;
    private QueueViewMBean monitoredQueue;
    private IServiceStatistics qstats;


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
    
    String nodeId;
    String pid;
    boolean gmfail = false;
    String  failure_reason = null;

    String jmxFailure = null;

    String broker_host;
    volatile boolean useNewJmxQueryString = false;
    public UimaAsServiceMonitor(String qname, String broker_host, int broker_jmx_port)
    {
        this.broker_host = broker_host;
    	this.qname = qname;
        this.broker_url = "service:jmx:rmi:///jndi/rmi://"+ broker_host + ":" + broker_jmx_port + "/jmxrmi";        
        this.qstats = new ServiceStatistics(false, false, "N/A");
    }

//     public ServiceStatistics getStatistics()
//     {
//         try {
//             collect();
//             qstats.setAlive(true);        // if we don't croak gathering stuff, we're not dead
//             qstats.setHealthy(true);
//             qstats.setInfo(format());
//         } catch ( Throwable t ) {
//             qstats.setAlive(false);        // if we don't croak gathering stuff, we're not dead
//             qstats.setHealthy(false);
//             qstats.setInfo(t.getMessage());
//         }
//     	return qstats;
//     }
    
    private ObjectName getBrokerObjectName(MBeanServerConnection conn, String brokerQuery, String brokerNameProperty ) throws Exception {
        ObjectName brokerObjectName = null;
    	for (Object nameObject : conn.queryNames(new ObjectName(brokerQuery), (QueryExp) null)) {
            //find the brokername object
            brokerObjectName = (ObjectName) nameObject;

            // the following code checks if the current object represents AMQ Broker MBean which looks like:
            // org.apache.activemq:type=Broker,brokerName=localhost
            // The above has just 2 properties: type and brokerName. We are after the brokerName here. Only
            // one MBean has just these two properties.
            Map<String,String> parts = brokerObjectName.getKeyPropertyList();
            boolean done = false;
            if ( parts.size() == 2) {  // looking for two properties Object only 
            	for( Entry<String,String> entry : parts.entrySet()) {
//                    if ( entry.getKey().equals("brokerName")) {   // need this property
                    if ( entry.getKey().equals(brokerNameProperty)) {   // need this property
                    	done = true;    // found a match. Got a broker ObjectName
                    	break;
                    }
            	}
            }
            if ( done ) {
            	break;
            }
//            if (brokerObjectName.getCanonicalName().endsWith("Type=Broker")) {
                // Extract just the name from the canonical name
                //String brokerName = brokerObjectName.getCanonicalName().substring(0, brokerObjectName.getCanonicalName().indexOf(","));
                //System.out.println("Canonical name of broker is " + brokerObjectName.getCanonicalName());
                //System.out.println("broker name is " + brokerName);
        }
    	return brokerObjectName;
    }
    
    /**
     * Connect to ActiveMq and find the mbean for the queue we're trying to monitor
     */
    public void init(String parm /* parm not used in this impl */)
        throws Exception
    {
        JMXServiceURL url = new JMXServiceURL(broker_url);
        jmxc = JMXConnectorFactory.connect(url);
        MBeanServerConnection conn = jmxc.getMBeanServerConnection();        
        String jmxDomain = "org.apache.activemq";  
        String brokerJmxDomain = null;
        
        //
        // First get the broker name. Assume we are connecting to AMQ broker version >= 5.8.0
        //
        String brokerNameProperty = "brokerName";  
        String destinationId = ",destinationType=Queue,destinationName=";
        ObjectName brokerObjectName = null;
        String brokerQuery = jmxDomain + ":type=Broker,*"; 
        // Get ObjectName for the Broker. The getBrokerObjectName() returns null if 
        // version of broker < 5.8.0
        brokerObjectName = getBrokerObjectName(conn, brokerQuery, brokerNameProperty);
        if ( brokerObjectName == null ) {  // broker version 5.7
            // this is a fall back mechanism in case we are connected to an older broker ( < 5.8.0).
        	// In older versions of AMQ query syntax is different. 
        	brokerQuery = jmxDomain + ":Type=Broker,*";
        	brokerNameProperty = "BrokerName";
        	destinationId = ",Type=Queue,Destination=";
            brokerObjectName = getBrokerObjectName(conn, brokerQuery, brokerNameProperty);
            String bcn = brokerObjectName.getCanonicalName();
            brokerJmxDomain = bcn.substring(0,bcn.indexOf(","));
        } else {
        	brokerJmxDomain = brokerObjectName.getCanonicalName();
        }
        ObjectName targetQueueON = new ObjectName(brokerJmxDomain+destinationId+qname);
        QueueViewMBean brokerQMBean = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, targetQueueON ,QueueViewMBean.class, true);
        monitoredQueue = brokerQMBean;
        
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

    public void resetStatistics()
    {
        try {
            init(null);
            
            if ( monitoredQueue != null ) {
                monitoredQueue.resetStatistics();
            }
            stop();
        } catch (Throwable t) {
            // Nothing .. we don't care if this fails; this is just
            // prophylaxis.  If there is really a problem it will show
            // up later.
        }
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

    public void setJmxFailure(String msg)
    {
        this.jmxFailure = msg;
    }

    public void setSource(String nodeId, String pid, boolean gmfail, String failure_reason)
    {
        this.nodeId = nodeId;
        this.pid = pid;
        this.gmfail = gmfail;
        this.failure_reason = failure_reason;
    }

    public void setSource(String nodeId, String pid, boolean gmfail)
    {
        this.nodeId = nodeId;
        this.pid = pid;
        this.gmfail = gmfail;
        this.failure_reason = null;
    }

    public IServiceStatistics getStatistics()
    {
        try {
            collect();
            qstats.setAlive(true);        // if we don't croak gathering stuff, we're not dead
            qstats.setHealthy(true);
            qstats.setInfo(format());
        } catch ( Throwable t ) {
            qstats.setAlive(false);        // if we don't croak gathering stuff, we're not dead
            qstats.setHealthy(false);
            qstats.setInfo(t.getMessage());
        }
    	return qstats;
    }

    public String format()
    {
        String answer = null;
        if ( jmxFailure != null ) {
            answer = "JMX Failure[" 
                + jmxFailure + "]" 
                +  "] MetaNode[" + nodeId
                +  "] MetaPid[" + pid
                ;

        } else {
            answer = "QDEPTH[" + queueSize
                +  "] AveNQ[" + new DecimalFormat("####.##").format(enqueueTime)
                +  "] Consum[" + consumerCount
                +  "] Prod[" + producerCount
                +  "] minNQ[" + minEnqueueTime
                +  "] maxNQ[" + maxEnqueueTime
                +  "] expCnt[" + expiredCount
                +  "] inFlt[" + inFlightCount
                +  "] DQ[" + dequeueCount
                +  "] NQ[" + enqueueCount
                +  "] NDisp[" + dispatchCount
                +  "] MetaNode[" + nodeId
                +  "] MetaPid[" + pid 
                +  "]"
                ;
        }

        if ( gmfail ) {
            if ( failure_reason == null ) {
                answer = answer + "; getMeta failure to service.";
            } else {
                answer = answer + ": " + failure_reason;
            }
        }
        return answer;
    }

    public void collect()
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
            monitoredQueue.resetStatistics();
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

    public long getQueueSize()
    {
        return queueSize;
    }

    /**
	 * @return the enqueueTime
	 */
	public double getEnqueueTime() {
		return enqueueTime;
	}

	/**
	 * @return the consumerCount
	 */
	public long getConsumerCount() {
		return consumerCount;
	}

	/**
	 * @return the producerCount
	 */
	public long getProducerCount() {
		return producerCount;
	}

	/**
	 * @return the minEnqueueTime
	 */
	public long getMinEnqueueTime() {
		return minEnqueueTime;
	}

	/**
	 * @return the maxEnqueueTime
	 */
	public long getMaxEnqueueTime() {
		return maxEnqueueTime;
	}

	/**
	 * @return the inFlightCount
	 */
	public long getInFlightCount() {
		return inFlightCount;
	}

	/**
	 * @return the dequeueCount
	 */
	public long getDequeueCount() {
		return dequeueCount;
	}

	/**
	 * @return the enqueueCount
	 */
	public long getEnqueueCount() {
		return enqueueCount;
	}

	/**
	 * @return the dispatchCount
	 */
	public long getDispatchCount() {
		return dispatchCount;
	}

	/**
	 * @return the expiredCount
	 */
	public long getExpiredCount() {
		return expiredCount;
	}

	/**
	 * @return the healthy
	 */
	public boolean isHealthy() {
		return healthy;
	}

	/**
	 * @return the gmfail
	 */
	public boolean isGmfail() {
		return gmfail;
	}

	/**
	 * @return the jmxFailure
	 */
	public String getJmxFailure() {
		return jmxFailure;
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
            IServiceStatistics qs = null;
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
