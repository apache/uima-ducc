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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;

/**
* Represent a single instance.  
*
* This is a simple class, mostly just a container for the state machine.
*/
class PingOnlyServiceInstance
    extends ServiceInstance
{
	private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);	
	private PingOnlyDriver driver = null;
	private PingOnlyServiceInstance me;
	
    PingOnlyServiceInstance(ServiceSet sset)
    {
    	super(sset);
    	me = this;
    }

//     void setState(DuccWorkJob dwj)
//     {
//         this.state = dwj.getJobState();
//     }

    long start(String spec, DuccProperties meta_props)
    {
    	String methodName = "start";

        // ping-only, we have no idea what the state of the remote service is, so we assume
        // it is running.  If not the ping will fail anyway, preventing dependent jobs from
        // using it.
        logger.info(methodName, sset.getId(), "START PING-ONLY INSTANCE");
        state = JobState.Running;
        setStopped(false);
        driver = new PingOnlyDriver();
        Thread driver_thread = new Thread(driver);
        driver_thread.start();
        return numeric_id;
    }


    /**
     * This assumes the caller has already verified that I'm a registered service.
     */
    void stop()
    {
        String methodName = "stop";
        logger.info(methodName, sset.getId(), "STOP PING-ONLY INSTANCE");
        state = JobState.Completed;
        driver.stop();
        setStopped(true);
    }

    /**
     * Must simulate state being driven in from the OR
     */
    class PingOnlyDriver 
        implements Runnable
    {
        boolean stopped = false;

        public synchronized void stop()
        {
            this.stopped = true;
        }

        public void run()
        {
        	String methodName = "PingOnlyDriver.run()";
            int delay = SystemPropertyResolver.getIntProperty("ducc.orchestrator.state.publish.rate", 30000);
            
            while ( true ) {
                if (stopped) return;

                try {
                    logger.info(methodName, sset.getId(), "Starts Wait of", delay);
					Thread.sleep(delay);
                    //logger.info(methodName, sset.getId(), "Returns");
				} catch (InterruptedException e) {
					// nothing
				}
                sset.signal(me);
            }
        }

        
    }

}
