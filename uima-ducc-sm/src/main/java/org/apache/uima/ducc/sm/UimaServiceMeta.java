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

import org.apache.uima.ducc.common.ServiceStatistics;
import org.apache.uima.ducc.common.UimaAsServiceMonitor;
import org.apache.uima.ducc.common.utils.DuccLogger;


/**
 * This class runs the watchdog thread that pings a service and maintains the
 * service state for a service.  There is one object in one thread per service.
 */

class UimaServiceMeta
    implements SmConstants,
               IServiceMeta
{	
    private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);	

    boolean shutdown = false;
    ServiceSet sset;
    String endpoint;
    String broker_host;
    int broker_port;

    int meta_ping_rate;
    int meta_ping_stability;

    UimaAsServiceMonitor monitor = null;
    boolean connected = false;

    UimaServiceMeta(ServiceSet sset, String endpoint, String broker_host, int broker_port)
    	throws Exception
    {
        this.sset = sset;
        this.endpoint = endpoint;
        this.broker_host = broker_host;
        this.broker_port = broker_port;
        this.meta_ping_rate = ServiceManagerComponent.meta_ping_rate;
        this.meta_ping_stability = ServiceManagerComponent.meta_ping_stability;
        this.monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_port);
    }

    public ServiceStatistics getServiceStatistics()
    {
        if ( monitor == null ) return null;
        return monitor.getServiceStatistics();
    }

    public void run()
    {
    	String methodName = "run";
        int missed_pings = 0;
        while ( true ) {
            synchronized(this) {
                if (shutdown) return;
            }

            if ( ! connected ) {
                try {
                    logger.debug(methodName, sset.getId(), "Initializing monitor");
                    monitor.connect();
                    connected = true;
                    logger.debug(methodName, sset.getId(), "Monitor initialized");
                } catch ( Throwable t ) {
                    connected = false;
                    logger.warn(methodName, sset.getId(), t);
                }
            }

            try {
                if ( connected ) {
                    monitor.collect();
                } else {
                    logger.error(methodName, sset.getId(), "Not collecting broler statistics, cannot connect to broker");
                }
            } catch ( Throwable t ) {
                logger.error(methodName, null, "Cannot collect broker statistics.  The queue may have been deleted.  Details:", t);
                connected = false;
            }


            if ( sset.ping() ) {
                synchronized(this) {
                    if (shutdown) return;
                    sset.setResponsive();
                }
                logger.info(methodName, sset.getId(), "Ping ok:", endpoint, monitor.getServiceStatistics().toString());
                missed_pings = 0;
            } else {
                logger.info(methodName, null, "missed_pings", missed_pings, "endpoint", endpoint);
                if ( ++missed_pings > meta_ping_stability ) {
                	logger.info(methodName, null, "Seting state to unresponsive, endpoint", endpoint);
                    sset.setUnresponsive();
                } else {
                	logger.info(methodName, null, "Seting state to waiting, endpoint", endpoint);
                    sset.setWaiting();
                }
            }

            try {
            	logger.trace(methodName, null, "sleeping", meta_ping_rate);
                Thread.sleep(meta_ping_rate);
            } catch ( Throwable t ) {
                // nothing
            }            
        }
    }

    public synchronized void stop()
    {
        shutdown = true;
    }
}

