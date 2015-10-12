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

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * This enum defines the classes and constants used in the db schema
 */

public interface DbConstants
{

    static final String DUCCID        = "ducc_dbid";                 // DB-unique name for the duccid
    static final String DUCC_DBCAT    = "ducc_dbcat";                // The ducc database category: history, checkpoint, sm registry
    static final String DUCC_DBNODE   = "ducc_dbnode";

    // for all vertices we need to know the base and the name
    public interface VSchema
    {
        String pname();           // the name of the ODB class
        VSchema parent();         // parent class, if any. if none, then V
        Index[] indices();        // indices to define on the class
    }

    // for all indices we need to know the name, the base class, the property, and the type
    public interface Index
    {
        String pname();       // index name
        String propname();    // name of the property it is applied to,, must exist in base
        OType  type();        // datatype
    }

    public enum DuccIndex
        implements Index 
    {
        IDuccId {
            public String pname()    { return "i_duccid"; }
            public String propname() { return DUCCID; }
            public OType  type()     { return OType.LONG; }
        },

        ICategory {
            public String pname()    { return "i_category"; }
            public String propname()    { return DUCC_DBCAT; }
            public OType  type()     { return OType.STRING; }
        },

        INodeName {
            public String pname()    { return "i_nodename"; }
            public String propname() { return DUCC_DBNODE; }
            public OType  type()     { return OType.STRING; }
        },
        ;
        
    }

    public enum DbCategory
    {
    	Any {
    		// All categories - don't qualify the search
    		public String pname() { return "all"; }
    	},
        Checkpoint {
            // OR checkpoint only
            public String pname() { return "checkpoint"; }
        },
        History {
            // Completed and deleted stuff, all classes of objects
            public String pname() { return "history"; }
        },
        SmReg {
            // Active service registration
            public String pname() { return "smreg"; }
        },
        RmState {
            // RM transient state.  Nodes, shares, etc.
            public String pname() { return "rmstate"; }
        },
        ;   
        public abstract String pname();
        
    }

    public enum DbVertex                        
        implements VSchema
    {
        VBase {
        	public String pname()    { return "VBase"; }
            public DbVertex parent() { return null; }
            public Index[] indices() { return new Index[] { DuccIndex.ICategory }; }
        },

        VWork {
        	public String pname()    { return "VWork"; }
            public DbVertex parent() { return VBase; }
            public Index[] indices() { return new Index[] { DuccIndex.IDuccId } ; }
        },

        RmNode {
        	public String pname()    { return "VRmNode"; }
            public DbVertex parent() { return VBase; }
            public Index[] indices() { return new Index[] { DuccIndex.INodeName }; }
        },

        //
        // The convention is for vertices to start with Capital V and then a Capital
        //
        Job {
            public String pname() { return "VJob"; } 
        },

        Classpath {         
            public String pname() { return "VClasspath"; } 
        }, 
            
        CommandLine {         
            public String pname() { return "VCommandLine"; } 
        }, 
            
        Driver {
            public String pname() { return "VJobDriver"; } 
        }, 
            
        Process {
            public String pname() { return "VProcess"; } 
        },
                                
        ServiceReg         {                                     // The submitted service properties
            public String pname() { return "VServiceReg"; } 
        },
 
        ServiceMeta     {                                     // The Service metadata
            public String pname() { return "VServiceMeta"; } 
        },
 
        ServiceInstance {                                     // The serialized service instance from OR
            public String pname() { return "VServiceInstance"; } 
        },
                                            
        Reservation     {                                     // The serialized reservation instance from OR
            public String pname() { return "VReservation"; } 
        },

        ProcessToJob     {                                     // For checkpoints, the process - to - job id map
            public String pname() { return "VProcessToJob"; } 
        },
        ;

        public DbVertex parent() { return VWork; }
        public Index[] indices() { return null; }
    }

    public interface ESchema
    {
        String pname();           // the name of the ODB class
        ESchema parent();
    }

    public enum DbEdge
        implements ESchema
    {
        //
        // The convention is for edges to start with lower e and then a lower
        //
        // Edge         {         // Generic edge
        //     public String pname() { return "ducc_edge"; } 
        // },

        EBase {
            public String pname() { return "ducc_ebase"; } 
            public ESchema parent() { return null; }
        },

        Classpath         {    // All record types, detached classpath
            public String pname() { return "eclasspath"; } 
        },
        Driver         {       // From DuccWorkJob
            public String pname() { return "edriver"; } 
        },
        JpProcess        {       // Process instance
            public String pname() { return "eprocess"; } 
        },
        JdProcess        {       // Process instance
            public String pname() { return "ejdprocess"; } 
        },
        ServiceMeta          { // Setvice meta file  
            public String pname() { return "eservice_meta"; } 
        },

        ;

        public ESchema parent() { return EBase; }

    }
}
