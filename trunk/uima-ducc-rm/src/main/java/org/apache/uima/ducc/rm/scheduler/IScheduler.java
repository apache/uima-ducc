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

import java.util.Map;

import org.apache.uima.ducc.rm.scheduler.SchedConstants.EvictionPolicy;


/**
 * Define the scheduling interface.  You can have any scheduler you want, as long
 * as it conforms to this thing.
 *
 * We're going to assume that the caller of this interface won't call it unless 
 * something has changed that might affect the current schedule.
 */
public interface IScheduler
{
    public void schedule(SchedulingUpdate upd);

    public void setClasses(Map<ResourceClass, ResourceClass> classes);        // classes

    public void setNodePool(NodePool nodepool);

    public void setEvictionPolicy(EvictionPolicy p);
}
