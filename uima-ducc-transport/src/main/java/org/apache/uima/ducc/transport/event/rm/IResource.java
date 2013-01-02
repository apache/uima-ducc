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

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;


/**
 * This interface defines exactly one "resource" or "share".  The ID is unique within the specific machine, so that
 * if two resource instances are found on the same machine, they can be uniquely identified.
 */
public interface IResource extends Serializable
{
    /**
     * Returns the unique id, of the share.  Share IDs last only as long as they're assigned to a job and
     * won't be reused once they are reclaimed (the job vacates the share).
     */
    DuccId getId();

    /**
     * Returns the node identity where the share resides.
     */
    NodeIdentity getNodeId();     // The node where this resource resides, as provided by the Node Agent

    /**
     * If true, this share has been purged because its node went AWOL.
     */
    boolean isPurged();

    /**
     * Returns the number of quantum shares this resource occupies.
     */
    int countShares();
}
