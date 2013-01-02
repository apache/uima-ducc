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

import java.io.Serializable;

public class ServiceStatistics
    implements Serializable
{

    
	/**
	 * 
	 */
	private static final long serialVersionUID = -1;
	private static final String colWidth = "6";
    private static final int    ncols = 10;
    private String headerFormat = "";
    private String dataFormat = "";

    long consumerCount;
    long producerCount;
    long queueSize;

	long queueTime;

    long minEnqueueTime;
    long maxEnqueueTime;
    long inFlightCount;
    long dequeueCount;

    long enqueueCount;
    long dispatchCount;
    long expiredCount;

    boolean ping;

    public ServiceStatistics()
    {
        StringBuffer hb = new StringBuffer();
        StringBuffer db = new StringBuffer();
        for ( int i = 0; i < ncols; i++ ) {
            hb.append("%");
            hb.append(colWidth);
            hb.append("s ");

            db.append("%");
            db.append(colWidth);
            db.append("d ");
        }
        headerFormat = hb.toString();
        dataFormat = db.toString();
    }

    public long getConsumerCount() {
		return consumerCount;
	}

	public void setConsumerCount(long consumerCount) {
		this.consumerCount = consumerCount;
	}

	public long getProducerCount() {
		return producerCount;
	}

	public void setProducerCount(long producerCount) {
		this.producerCount = producerCount;
	}

	public long getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(long queueSize) {
		this.queueSize = queueSize;
	}

	public long getQueueTime() {
		return queueTime;
	}

	public void setQueueTime(long queueTime) {
		this.queueTime = queueTime;
	}

	public long getMinEnqueueTime() {
		return minEnqueueTime;
	}

	public void setMinEnqueueTime(long minEnqueueTime) {
		this.minEnqueueTime = minEnqueueTime;
	}

	public long getMaxEnqueueTime() {
		return maxEnqueueTime;
	}

	public void setMaxEnqueueTime(long maxEnqueueTime) {
		this.maxEnqueueTime = maxEnqueueTime;
	}

	public long getInFlightCount() {
		return inFlightCount;
	}

	public void setInFlightCount(long inFlightCount) {
		this.inFlightCount = inFlightCount;
	}

	public long getDequeueCount() {
		return dequeueCount;
	}

	public void setDequeueCount(long dequeueCount) {
		this.dequeueCount = dequeueCount;
	}

	public long getEnqueueCount() {
		return enqueueCount;
	}

	public void setEnqueueCount(long enqueueCount) {
		this.enqueueCount = enqueueCount;
	}

	public long getDispatchCount() {
		return dispatchCount;
	}

	public void setDispatchCount(long dispatchCount) {
		this.dispatchCount = dispatchCount;
	}

	public long getExpiredCount() {
		return expiredCount;
	}

	public void setExpiredCount(long expiredCount) {
		this.expiredCount = expiredCount;
	}

    public void setPing(boolean p)
    {
        this.ping = p;
    }

    public boolean getPing()
    {
        return ping;
    }

    public String header()
    {
        return String.format(headerFormat,
                             "Consum",
                             "Prod",
                             "Qsize",
                             "minNQ",
                             "maxNQ",
                             "expCnt",
                             "inFlgt",
                             "DQ",
                             "NQ",
                             "Disp");                             
        
    }

    public String toString()
    {
        return String.format(dataFormat,
                             consumerCount,  
                             producerCount,  
                             queueSize,      
                             minEnqueueTime, 
                             maxEnqueueTime,
                             expiredCount,
                             inFlightCount,  
                             dequeueCount,   
                             enqueueCount,   
                             dispatchCount
                             );  
 
    }

}
