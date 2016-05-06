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
package org.apache.uima.ducc.transport.event.sm;

import java.io.Serializable;

/**
 * Service related constants that are public.
 * 
 * Important: Do not put any methods into this interface.  It is a holder for constants only.
 */
public interface IService
    extends Serializable
{
    static final String NULL = "<none>";     

    //
    // For modification of boolean options, we want a trinary: true, false, unset
    //
    public enum Trinary
    {
        True   { public boolean decode() { return true; }},
        False  { public boolean decode() { return false; }},
        Unset  { public boolean decode() { throw new IllegalStateException("decode() is illegal for Trinary."); }} // must check explicitly, not decode  
        ;
        
        public abstract boolean decode();

      	public static Trinary encode(String value)
        {
            if ( value.equals("true") ) return True;
            if ( value.equals("false") ) return False;
            return Unset;
        }
    };

    public enum ServiceType
    {
        //
        // I want to expose these things to the API with the ugly upper-case notation but don't
        // want that ugliness in the variables, so we have encode and decode routines to do the
        // correct translation.
        //
        UimaAs      { public String decode() { return "UIMA-AS"; } },
        Custom      { public String decode() { return "CUSTOM" ; } },
        Undefined   { public String decode() { return "UNDEFINED"; } },
        ;
        
        public abstract String decode();

        public static ServiceType encode(String value)
        {
            if ( value.equals("UIMA-AS") ) return UimaAs;
            if ( value.equals("CUSTOM") ) return Custom;
            return Undefined;
        }
    };

    public enum ServiceClass
    {
        //
        // I want to expose these things to the API with the ugly upper-case notation but don't
        // want that ugliness in the variables, so we have encode and decode routines to do the
        // correct translation.
        //
        Implicit       { public String decode() { return "Implicit"; } },
        Submitted      { public String decode() { return "Submitted" ; } },
        Registered     { public String decode() { return "Registered"; } },
        Custom         { public String decode() { return "Custom"; } },
        Undefined      { public String decode() { return "Undefined"; } },
        ;
        
        public abstract String decode();

        public static ServiceClass encode(String value)
        {
            if ( value.equals("Implicit"  ) ) return Implicit;
            if ( value.equals("Submitted" ) ) return Submitted;
            if ( value.equals("Registered") ) return Registered;
            if ( value.equals("Custom") )     return Custom;
            return Undefined;
        }
    };

	public enum ServiceState 
	{
        //
        // These things end up as strings in a props file where the custom is lower-case with - to separate words.
        // In code we want to be able to use the nicer mixed-case names.
        // Always use encode and decode and you can't get this wrong.
        //
        Pending           { public String decode() { return "pending"        ; } },  // Work is waiting on at least one service to start but
                                                                                     // the service is not disabled or some such.  UIMA-4223

		Waiting           { public String decode() { return "waiting"        ; } },  // A job is waiting on at least one service to ping
        Starting          { public String decode() { return "starting"       ; } },  // Instance is started, but not yet to Initializing
		Initializing      { public String decode() { return "initializing"   ; } },  // A job is waiting on at least one service to initialize
		Available         { public String decode() { return "available"      ; } },  // All services for this job are active and pinging, or else
                                                                                     //     no services are needed for the job
        NotAvailable      { public String decode() { return "not-available"  ; } },  // SM to OR only: reference to a non-existent service 
        Stopped           { public String decode() { return "stopped"        ; } },  // (newsm) The service is not started
        Stopping          { public String decode() { return "stopping"       ; } },  // Service is told to stop but it takes a while
        Undefined         { public String decode() { return "undefined"      ; } },  // Catch-all, means basically "who cares"
       ;

        public abstract String decode();

        public static ServiceState encode(String value)
        {
            if ( value.equals("pending"       ) ) return Pending;                    // UIMA-4223
            if ( value.equals("waiting"       ) ) return Waiting;
            if ( value.equals("starting"      ) ) return Starting;
            if ( value.equals("stopped"       ) ) return Stopped;
            if ( value.equals("initializing"  ) ) return Initializing;
            if ( value.equals("available"     ) ) return Available;
            if ( value.equals("not-available" ) ) return NotAvailable;
            if ( value.equals("stopping"      ) ) return Stopping;
            if ( value.equals("undefined"     ) ) return Undefined;
            return Undefined;
        }

        // used to accumulate multiple service states into a single value
        public int ordinality()
        {
            switch ( this ) {
                case Pending:      return 9;  // UIMA-4223 waiting for dependent service to become available
                case Available:    return 8;
                case Waiting:      return 7;
                case Initializing: return 6;
                case Starting:     return 5;
                case Stopping:     return 4;
                case Stopped:      return 3;
                case NotAvailable: return 2;
                case Undefined:    return 1;
                default:           return 0;
            }
        }
	};
};
