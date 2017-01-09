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

import java.io.Serializable;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


/**
 * This interface describes the scheduling state for one job, reservation, or service request.
 */
public interface IRmJobState extends Serializable
{
    /**
     * Returns the the unique id of the job, reservation, or service request as assigned during submit, and passed in from JM
     */
    public DuccId getId();
    public DuccType getDuccType();    // for messages :(

    /**
     * key is the share id, unique, as assigned by the RM
     * value is the resource aka share
     *
     * On each update, the shares may
     *          a) stay the same
     *          b) increase - new shares added
     *          c) decrease - some shares removed
     *          d) increase and decrease - some shares removed and some shares added
     */
    Map<DuccId, IResource> getResources(); // the shares assigned to the job, if any

    /**
     * The resource manager must not remove shares from its own records until the JM has
     * confirmed they are gone because there is an arbitrary period of time between the RM
     * deallocating them and the resource becoming physically available.
     *
     * This interface returns the set of resources for each job that must be deallocated.  They
     * remain in RM's records as in-use until a JM state update confirms they have been removed.
     *
     * Key is the id of a share..
     * Value is the share information as an IResource.
     */
    Map<DuccId, IResource> getPendingRemovals();

    /**
     * The RM marks a resource as busy immediately upon allocating it, altough there is
     * an arbitrary period of time before it becomes actually busy.  For each job with
     * expanded resources, this list spells out exactly which resources are to be added.
     *
     * Key is the id of a share.
     * Value is the share information as an IResource.
     */
    Map<DuccId, IResource> getPendingAdditions();

    /**
     * If RM has to refuse to handle a job, this flag says that.
     */
    boolean isRefused();

    /**
     * Actual memory allocated per process.
     */
    public int memoryGbPerProcess();

    /**
     * If isRefused() is true, this string contains a message explaining why.
     */
    String getReason();
}

