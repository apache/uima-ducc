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
package org.apache.uima.ducc.database;

import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;


/**
 * This is a helper to serialize the ProcessToJob map in the OR checkpoint.  We need this because
 * the key for that map is complex (a DuccId) and it can't be propertly serialized into a JSON
 * dictionary key.
 *
 */
class ProcessToJobList
{
    ArrayList<PjPair> l = new ArrayList<PjPair>();

    ProcessToJobList() {};
    ProcessToJobList(Map<DuccId, DuccId> m)
    {
        for ( DuccId k : m.keySet() ) {
            l.add(new PjPair(k, m.get(k)));
        }
    }

    void fill(Map<DuccId, DuccId> ptj)
    {
        for ( PjPair p : l ) ptj.put(p.k, p.v);
    }

    static private class PjPair
    {
        DuccId k;
        DuccId v;
        
        PjPair(DuccId k, DuccId v) { this.k = k; this.v = v; }
        
    }

}
