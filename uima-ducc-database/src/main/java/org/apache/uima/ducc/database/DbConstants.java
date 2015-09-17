package org.apache.uima.ducc.database;

/**
 * This enum defines the classes and constants used in the db schema
 */

public interface DbConstants
{

    static final String DUCCID    = "ducc_dbid";                 // DB-unique name for the duccid
    static final String DUCC_DBCAT = "ducc_dbcat";                // The ducc database category: history, checkpoint, sm registry

    public interface Schema
    {
        String pname();
    }

    public enum DbCategory
    	implements Schema
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

        ;
    }

    // Every vertex must inherit from here so we can use common indexes
    public enum DuccVertexBase
        implements Schema
    {
        VBase {
            public String pname() { return "VDuccBase"; } 
        },
            ;
    }

    public enum DbVertex                        
        implements Schema
    {
        //
        // The convention is for vertices to start with Capital V and then a Capital
        //
        Job {                                                  // The serialized job instance from OR
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

    }

    public enum DuccEdgeBase
        implements Schema
    {
        EdgeBase {
            public String pname() { return "ducc_ebase"; } 
        },
            ;
    }

    public enum DbEdge
        implements Schema
    {
        //
        // The convention is for edges to start with lower e and then a lower
        //
        // Edge         {         // Generic edge
        //     public String pname() { return "ducc_edge"; } 
        // },

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

    }
}
