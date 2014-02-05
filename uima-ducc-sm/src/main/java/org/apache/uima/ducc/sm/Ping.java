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
package org.apache.uima.ducc.sm;

import java.io.Serializable;
import java.util.Properties;

/**
 * This is the packet sent to the external pinger soliciting a response.
 */

class Ping
    implements Serializable
{
	private static final long serialVersionUID = 1L;
	boolean quit = false;
//     int instances= 0;
//     int references = 0;
//     int runFailures = 0;
    Properties smState;

    public Ping(boolean quit, Properties props)
    {
        this.quit = quit;
        this.smState = props;
    }

//     public Ping(boolean quit, int instances, int references, int runFailures)
//     {
//         this.quit = quit;
//         this.instances= instances;
//         this.references = references;
//         this.runFailures = runFailures;
//     }

    public boolean isQuit()          { return quit; }
//     public int     getInstances()    { return instances; }
//     public int     getReferences()   { return references; }
//     public int     getRunFailures()  { return runFailures; }
    public Properties getSmState  () { return smState; }
}
