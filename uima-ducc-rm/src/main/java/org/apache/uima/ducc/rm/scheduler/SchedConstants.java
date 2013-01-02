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
package org.apache.uima.ducc.rm.scheduler;

public interface SchedConstants
{
    public enum Policy {
        FAIR_SHARE,
        FIXED_SHARE,
        RESERVE,
    };

    public enum EvictionPolicy {
        SHRINK_BY_MACHINE,         // shrink by largest machine first, to minimize fragmentation
        SHRINK_BY_INVESTMENT,      // shrink by lowest investment first, to minimize waste
    };

    public static final String COMPONENT_NAME            = "RM";
    public static final int DEFAULT_STABILITY_COUNT      = 5;
    public static final int DEFAULT_INIT_STABILITY_COUNT = 3;
    public static final int DEFAULT_SCHEDULING_RATIO     = 4;
    public static final int DEFAULT_SCHEDULING_RATE      = 60000;
    public static final int DEFAULT_NODE_METRICS_RATE    = 60000;

    public static final int DEFAULT_PROCESSES            = 10;     // for jobs, number of processes if not specified
    public static final int DEFAULT_INSTANCES            = 1;     // for reservations, number of instances if not specified

    public static final int DEFAULT_MAX_PROCESSES        = Integer.MAX_VALUE; // for jobs, the class cap, if not configured
    public static final int DEFAULT_MAX_INSTANCES        = Integer.MAX_VALUE; // for reservations, class cap, if not configured

    public static final int DEFAULT_SHARE_WEIGHT         = 100;
    public static final int DEFAULT_PRIORITY             = 10;

}
            
