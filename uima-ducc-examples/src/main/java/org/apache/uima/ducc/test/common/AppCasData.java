/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.test.common;

import java.util.StringTokenizer;

import org.apache.uima.cas.CAS;

/**
 * Common code to parse and contain the CAS data that we bop around for these tests.
 */

public class AppCasData
{
    long   elapsed;
    int    task_id;
    int    total;
    double error_rate;
    String logid;      
    
    public AppCasData(CAS cas)
    {
        String data = cas.getSofaDataString();

        //
        // Parms are in a single multi-token string:
        //   elapsed time in MS for this WI
        //   task id
        //   total tasks
        //   simulated error rate.
        //   where to write the AE output
        //
        StringTokenizer tok = new StringTokenizer(data);

        elapsed    = Long.parseLong(tok.nextToken());
        task_id    = Integer.parseInt(tok.nextToken());
        total      = Integer.parseInt(tok.nextToken());
        error_rate = Double.parseDouble(tok.nextToken());
        logid      = tok.nextToken();
    }

    public String toString()
    {
        return "SLEEP TEST CAS:" +
               " elapsed    :"       + elapsed +
               " taskid     :"       + task_id +
               " total      :"       + total   +
               " error_rate :"       + error_rate +
               " logid      :"       + logid
            ;
    }
}
