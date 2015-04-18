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
package org.apache.uima.ducc.common.admin.event;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This object represents, for each class, the number of shares that are being requested of it, and
 * the number of shares that have been awarded to requestsin this class, broken down by multiples
 * of the share quantum (in essence number of processes broken down by size).
 */
public class RmQueriedClass
	implements Serializable
{
	private static final long serialVersionUID = 1L;

    private String name;
    private String policy;
    private int[] demanded;
    private int[] awarded;

    public RmQueriedClass()
    {
    }

    /**
     * @return the name of the class.
     */
	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}

    /**
     * @return the scheduling policy for the class:
     *         FAIR_SHARE
     *         FIXED_SHARE
     *         RESERVE
     */
    public String getPolicy() {
        return policy;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
    }

    /**
     * @return an array, indexed by multiples of the share quantum, showing the number of requests of each size
     *         currently demanded by the workload.
     */
	public int[] getDemanded() {
		return demanded;
	}


	public void setDemanded(int[] demanded) {
		this.demanded = demanded;
	}


    /**
     * @return an array, indexed by multiples of the share quantum, showing the number of requests of each size
     *         currently awared to requests in the class.
     */
	public int[] getAwarded() {
		return awarded;
	}


	public void setAwarded(int[] awarded) {
		this.awarded = awarded;
	}

    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("{ 'name': '");
        sb.append(name);
        sb.append("',\n'policy': '");
        sb.append(policy);
        sb.append("',\n 'requested': ");
        sb.append(Arrays.toString(demanded));
        sb.append(",\n'awarded': ");
        sb.append(Arrays.toString(awarded));
        sb.append(",\n}");

        return sb.toString();
    }
    
}
