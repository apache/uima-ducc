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

/**
 * Service related constants that are public.
 * 
 * Important: Do not put any methods into this interface.  It is a holder for constants only.
 */
public interface IService
{
    static final String NULL = "<none>";     
    public static enum ServiceCode 
    {
        OK,                   // API: The requested action succeeded
        NOTOK                 // API: the Requested action did not succeed.
    };

    //
    // For boolean options, we want a trinary: true, false, unset
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

    public enum ServiceVerb
    {
        //
        // I want to expose these things to the API with the ugly upper-case notation but don't
        // want that ugliness in the variables, so we have encode and decode routines to do the
        // correct translation.
        //
        Register    { 
            public String decode()      { return "register"; } 
            public String description() { return "Register a service."; } 
            public String argname()     { return "service-DD-specification."; } 
        },
        Unregister  { 
            public String decode()      { return "unregister" ; } 
            public String description() { return "Unregister a service." ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
        },
        Start       { 
            public String decode()      { return "start"; } 
            public String description() { return "Start a registered service." ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            },
        Stop        { 
            public String decode()      { return "stop"; } 
            public String description() { return "Stop a registered service." ; } 
            public String argname()     { return "wervice-id-or-endpoint [--instances number-to-stop]" ; } 
        },
        Modify      { 
            public String decode()      { return "modify"; } 
            public String description() { return "Modify meta properties for a registered service." ; } 
            public String argname()     { return "modify-parameters" ; } 
        },
        Query       { 
            public String decode()      { return "query"; } 
            public String description() { return "Query registered services." ; } 
            public String argname()     { return "none" ; } 
        },
        Help        { 
            public String decode()      { return "help"; } 
            public String description() { return "This help message." ; } 
            public String argname()     { return "none" ; } 
        },
        Debug       { 
            public String decode()      { return "debug"; } 
            public String description() { return "Debug cli" ; } 
            public String argname()     { return "none" ; } 
        },
        Unknown     { 
            public String decode()      { return "unknown"; } 
            public String description() { return "unknown" ; } 
            public String argname()     { return "unknown" ; } 
        },
        ;

        public abstract String decode();
        public abstract String description();
        public abstract String argname();
        
        public static ServiceVerb encode(String value)
        {
            if ( value.equals("register") )   return Register;
            if ( value.equals("unregister") ) return Unregister;
            if ( value.equals("start") )      return Start;
            if ( value.equals("stop") )       return Stop;
            if ( value.equals("modify") )     return Modify;
            if ( value.equals("query") )      return Query;
            if ( value.equals("help") )       return Help;
            if ( value.equals("debug") )      return Debug;

            return Unknown;
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
        Implicit       { public String encode() { return "Implicit"; } },
        Submitted      { public String encode() { return "Submitted" ; } },
        Registered     { public String encode() { return "Registered"; } },
        Custom         { public String encode() { return "Custom"; } },
        Undefined      { public String encode() { return "Undefined"; } },
        ;
        
        public abstract String encode();

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
		Waiting           { public String decode() { return "waiting"        ; } },  // A job is waiting on at least one service to ping
		Initializing      { public String decode() { return "initializing"   ; } },  // A job is waiting on at least one service to initialize
		Available         { public String decode() { return "available"      ; } },  // All services for this job are active and pinging, or else
                                                                                     //     no services are needed for the job
        NotAvailable      { public String decode() { return "not-available"  ; } },  // At least one service is not available and can't be made available
        Stopping          { public String decode() { return "stopping"       ; } },  // Service is told to stop but it takes a while
        Undefined         { public String decode() { return "undefined"      ; } },  // Catch-all, means basically "who cares"
       ;

        public abstract String decode();

        public static ServiceState encode(String value)
        {
            if ( value.equals("waiting"       ) ) return Waiting;
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
                case Available:    return 6;
                case Initializing: return 5;
                case Waiting:      return 4;
                case Stopping:     return 3;
                case NotAvailable: return 2;
                case Undefined:    return 1;
                default:           return 0;
            }
        }
	};
};
