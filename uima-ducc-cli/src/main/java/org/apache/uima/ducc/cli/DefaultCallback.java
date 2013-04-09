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
package org.apache.uima.ducc.cli;


public class DefaultCallback implements IDuccCallback {
    private String host;
    
    public void duccout(String s) 
    { 
        if ( host == null ) {
            System.out.println( s);
        } else {
            System.out.println("[" + host + "] " + s); 
        }
    }

    public void consout(String s) 
    { 
        System.out.println("[" + host + "] " + s); 
    }

    @Override
    public void host(String name) {
        host = name;
    }

    @Override
    public void logfile(String name) {
    }
}


