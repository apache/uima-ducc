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
package org.apache.uima.ducc.transport.event.rm;

import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;


/**
 * This interface defines one state event from teh resource manager.  It is a list of the states of all
 * known jobs, reservations, and service requests.
 */
public interface IRmStateEvent
{

    /**
     * Returns a map of the RM's view of the world.
     * 
     * Key is the job's duccid as received from JM
     * Value is RmJobState, which is the set of shares currently assigned to the job.
     *
     */
    Map<DuccId, IRmJobState> getJobState();        // all the jobs rm knows about

}


