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

import java.util.Comparator;


/**
 * A SchedulingEntity is one of a collection of similar objects that compete for the same set
 * of resources.  For instance -
 *
 * The collection of ResourceClasses is a set of ScheduingEntities competing for all the resources
 * that are still left after scheduling higher priority work.
 *
 * A collection of Users is a set of SchedulingEntities competing for all the resouces assigned
 * to a specific ResourceClass.
 *
 * A collection of Jobs is a set of SchedulingEntities competing for all the resources assigned to
 * a specific user.
 * 
 * Note that this interface does NOT fully define any concrete entity so we don't have or want a
 * full beany-like get/set interface.  
 */
interface IEntity
{
    int    getShareWeight();           // the fair-share weight of this entity within its collection
                                       // the setter isn't required, must come some other way

    String getName();                  // the name / id of the entity (for messages)
                                       // the setter isn't required, name must be set some other way

    void   initWantedByOrder(ResourceClass rc);
    int[]  getWantedByOrder();         // the number of resources of each order wanted by this entity
                                       // setter isn't required, often an entity will produce this by
                                       //   calculation anyway

    int[]  getGivenByOrder();          // the number of resources actually allocated after scheduling for this entity.
    void   setGivenByOrder(int[] gbo); // the scheduler uses this to set the allocation after each
                                       //    scheduling round

    int    calculateCap(int order, int basis); // The entity must work out any caps that may restrict the counts

    long   getTimestamp();                   // for tiebreaks

    Comparator<IEntity> getApportionmentSorter();
}
