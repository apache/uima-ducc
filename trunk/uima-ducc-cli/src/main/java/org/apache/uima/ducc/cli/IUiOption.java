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

/**
 * This interface defines the rules by which every command-line token must conform.
 *
 * The interface is intended (but not required) to be used as a base
 * for an Enum whose members define each keyword token.  For example:
 * <code>
 *   enum UiOption
 *       implements IUiOption
 *   {
 *       Activate   { 
 *           public String  pname()       { return "activate"; } 
 *           public String  argname()     { return null; }
 *           public boolean optargs()     { return true; }
 *           public String  description() { return "If present, apply current service updates to the running instances.."; } 
 *           public String  example()     { return null; }
 *       },
 *       
 *       AllInOne   { 
 *           public String pname()       { return "all_in_one"; } 
 *           public String argname()     { return "local|remote"; } 
 *           public String description() { return "Run driver and pipeline in single process."; } 
 *           public String example()     { return null; }
 *       },
 *       public boolean multiargs() { return false; } // the option can have multiple arguments
 *       public boolean required()  { return false; } // this option is required
 *       public String  deflt()     { return null; }  // default, or ""
 *       public String  sname()     { return null; }  // short name of option
 *       public boolean optargs()   { return false; } // is the argument optional?
 *
 *       public String makeDesc()
 *       {
 *           if ( example() == null ) return description();
 *           return description() + "\nexample: " + example();
 *       }
 *   };
 *
 * </code>
 */
public interface IUiOption
{
    
    /**
     * Some sanity checking is done by the parser before parsing starts, to insure
     * consistency of the parse and argument specification:
     *
     * If multiargs is true, 
     *    noargs  MUSt be false
     *    optargs MAY  be true
     * If noargs is true,
     *    multargs MUSt be false
     *    optargs  MUSt be false
     * If optargs is true
     *    multargs MAY be true
     *    noargs   MUST be false
     *    deflt    must be non-null, non-empty-string
     */
    // Each option must implement the first few methods,
    public String  pname();             // name of the option, e.g.  --description
    public String  argname();           // type of its argument, or null if none  
    public String  description();       // description of what it is              
    public String  example();           // example of usage                       
    public boolean multiargs();         // the option can have >1 arg
    public boolean required();          // this option is required
    public boolean optargs();           // the arguments are optional
    public boolean noargs();            // no arguments allowed
    public String  deflt();             // default, if any
    public String  sname();             // short name of option, if any
        
}
