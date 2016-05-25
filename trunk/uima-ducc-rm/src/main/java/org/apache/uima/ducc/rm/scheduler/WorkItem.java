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

public class WorkItem
{
    private int id;
    //private int cputime;
    private int walltime;

    public WorkItem(int id, int walltime)
    {
        // not checking these because they come from generated code that we assume is correct
        this.id = id;
        this.walltime = walltime;
    }

    int getId()
    {
        return id;
    }

    String getStringId()
    {
        // convenience, for logging
        return Integer.toString(id);
    }

    int getWalltime()
    {
    	return walltime;
    }
    
    public String toString()
    {
        return "Qid " + id + " " + walltime;
    }
}
