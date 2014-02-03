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

import org.apache.uima.ducc.transport.event.sm.IService;

/**
 * Constants that are private to SM.
 */
public interface SmConstants
    extends IService
{            
    // TODO: fold this into IService if we don't find anything else that belongs here.
    public final static String COMPONENT_NAME = "SM";


//     public enum ServiceEvent
//     {
//         Started,           // A service instance started
//         StartFailure,      // A service instance failed to start (from OR)
//         Initializing,      // OR has recognized it but it's not yet Running
//         Running,           // A service instance is successfuly running, according to OR
//         RunFailure,        // A service instances failed after successful start
//         PingSuccess,       // The service has been happily pung
//         PingFailure,       // The pinger is reporting trouble
//         Stopping,          // This service is requested to stop but still has active instances or pringer
//         Stopped,           // A service instance is fully stopped
//     };

    //
    // decode() mean convert the enum into the string the user uses
    // encode() means take the user's string and turn it into th enum
    // description() is a short description of the option for the commons cli parser
    // argname()     is a name for the argument for the usage() part of cli parser
    //
    public enum ServicePing
    {
        Arguments { 
            public String decode()      { return "arguments"; }
            public String description() { return "(Optional) arg string for the pinger."; }
            public String argname()     { return "string"; }
        },            

        Class { 
            public String decode()      { return "class"; }
            public String description() { return "Class implementing AServicePing to use for the pinger."; }
            public String argname()     { return "classname"; }
        },            

        Endpoint { 
            public String decode()      { return "endpoint"; }
            public String description() { return "Custom endpoint name, passed to the class in case it cares."; }
            public String argname()     { return "string"; }
        },            

        Port { 
            public String decode()      { return "port"; }
            public String description() { return "The port of the service manager handler for this custom pinger."; }
            public String argname()     { return "integer"; }
        },            

        Unknown {
            public String decode()      { return "unknown"; }
            public String description() { return "Illegal argument"; }
            public String argname()     { return "none"; }
        },
        ;
        
        public abstract String decode();
        public abstract String description();
        public abstract String argname();

        public static ServicePing  encode(String value)
        {
            if ( value.equals("arguments") )  return Arguments;
            if ( value.equals("class") )      return Class;
            if ( value.equals("endpoint") )   return Endpoint;
            if ( value.equals("port") )       return Port;
            return Unknown;
        }

    };
}
