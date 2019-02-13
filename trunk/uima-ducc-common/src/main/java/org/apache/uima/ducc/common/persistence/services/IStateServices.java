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
package org.apache.uima.ducc.common.persistence.services;

import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public interface IStateServices {

	public static String svc_reg_dir = IDuccEnv.DUCC_STATE_SVCREG_DIR;
	public static String svc_hist_dir = IDuccEnv.DUCC_HISTORY_SVCREG_DIR;

	public static final String svc = "svc";
	public static final String meta = "meta";

    public static final String UIMA_AS = "UIMA-AS";
    public static final String CUSTOM = "CUSTOM";

    public static final String sequenceKey = "service.seqno";

    // IMPORTANT IMPORTANT IMPORANT
    // For DB: we must distinguish between properties for the registration itself, and
    //         metaprops.
    // IMPORTANT IMPORTANT IMPORANT

    // All this will be strings I guess, because they're all entered by humans
    // and all the code is already debugged to deal with conversion.
    public enum SvcRegProps 
        implements IDbProperty
    {
    	TABLE_NAME {
            public String pname()      { return "smreg"; } 
            public Type type()         { return Type.String; }
            public boolean isPrivate() { return true; }    		
            public boolean isMeta() { return true; }    		
    	},
    	
        numeric_id  { 
            public String pname()         { return "numeric_id"; }
            public boolean isPrimaryKey() { return true; }
            public boolean isPrivate()    { return true; }

        },
         
        uuid {
            public String pname()         { return "uuid"; } 
            public boolean isPrivate()    { return true; }
        }, 

        is_archived {
            public String pname()      { return "is_archived"; } 
            public Type type()         { return Type.Boolean; }
            public boolean isPrivate() { return true; }
        },

        description                     { public String pname() { return "description"; } },
        administrators                  { public String pname() { return "administrators"; } },
        scheduling_class                { public String pname() { return "scheduling_class"; } },
        log_directory                   { public String pname() { return "log_directory"; } },
        working_directory               { public String pname() { return "working_directory"; } },
        jvm                             { public String pname() { return "jvm"; } },
        process_jvm_args                { public String pname() { return "process_jvm_args"; } },
        classpath                       { public String pname() { return "classpath"; } },
        environment                     { public String pname() { return "environment"; } },

        process_memory_size             { public String pname() { return "process_memory_size"; } },
        process_dd                      { public String pname() { return "process_descriptor_DD"; } },
        process_debug                   { public String pname() { return "process_debug"; } },
        process_executable              { public String pname() { return "process_executable"; } },
        process_executable_args         { public String pname() { return "process_executable_args"; } },
        process_initialization_time_max { public String pname() { return "process_initialization_time_max"; } },

        service_dependency              { public String pname() { return "service_dependency"; } },
        service_request_endpoint        { public String pname() { return "service_request_endpoint"; } },
        service_linger                  { public String pname() { return "service_linger"; } },

        service_ping_arguments          { public String pname() { return "service_ping_arguments"; } },
        service_ping_class              { public String pname() { return "service_ping_class"; } },
        service_ping_classpath          { public String pname() { return "service_ping_classpath"; } },
        service_ping_jvm_args           { public String pname() { return "service_ping_jvm_args"; } },
        service_ping_dolog              { public String pname() { return "service_ping_dolog"; } },
        service_ping_timeout            { public String pname() { return "service_ping_timeout"; } },

        instance_failures_window        { public String pname() { return "instance_failures_window"; } },
        instance_failures_limit         { public String pname() { return "instance_failures_limit"; } },
        instance_init_failures_limit    { public String pname() { return "instance_init_failures_limit"; } },

        ;
        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }
    };

    // These will be strings as well because there's code all over assuming they're strings.
    public enum SvcMetaProps 
        implements IDbProperty
    {
    	TABLE_NAME {
            public String pname()      { return "smmeta"; } 
            public Type type()         { return Type.String; }
            public boolean isPrivate() { return true; }    		
            public boolean isMeta() { return true; }    		
    	},

        numeric_id { 
            public String pname() { return "numeric_id"; }
            public boolean isPrimaryKey() { return true; }
        },

        is_archived {
            public String pname() { return "is_archived"; } 
            public Type type() { return Type.Boolean; }
            public boolean isPrivate() { return true; }
        },

        uuid                { public String pname() { return "uuid"; } }, 

        reference           { public String pname() { return "reference"; } },     // boolean, is this reference s
        enabled             { public String pname() { return "enabled"; } },
        autostart           { public String pname() { return "autostart"; } },     // here we specify whether it is autostarted
        instances           { public String pname() { return "instances"; } },
        endpoint            { public String pname() { return "endpoint"; } },

        disable_reason      { 
            public String pname() { return "disable-reason"; } 
            public String columnName() { return "disable_reason"; } 
        },

        implementors        { 
            public String pname() { return "implementors"; } 
        },

        ping_active         { 
            public String pname() { return "ping-active"; } 
            public String columnName() { return "ping_active"; } 
        },
        ping_only           { 
            public String pname() { return "ping-only"; } 
            public String columnName() { return "ping_only"; } 
        },

        service_alive       { 
            public String pname() { return "service-alive"; } 
            public String columnName() { return "service_alive"; } 
        },
        service_class       { 
            public String pname() { return "service-class"; } 
            public String columnName() { return "service_class"; } 
        },
        service_dependency  { 
            public String pname() { return "service-dependency"; } 
            public String columnName() { return "service_dependency"; } 
        },
        service_healthy     { 
            public String pname() { return "service-healthy"; } 
            public String columnName() { return "service_healthy"; } 
        },
        service_state       { 
            public String pname() { return "service-state"; } 
            public String columnName() { return "service_state"; } 
        },
        last_use            { 
            public String pname() { return "last-use"; } 
            public String columnName() { return "last_use"; } 
        },
        last_use_readable   { 
            public String pname() { return "last-use-readable"; } 
            public String columnName() { return "last_use_readable"; } 
        },
        service_statistics  { 
            public String pname() { return "service-statistics"; } 
            public String columnName() { return "service_statistics"; } 
        },
        service_type        { 
            public String pname() { return "service-type"; } 
            public String columnName() { return "service_type"; } 
        },
        submit_error        { 
            public String pname() { return "submit-error"; } 
            public String columnName() { return "submit_error"; } 
        },
        user                { 
            public String pname() { return "user"; } 
        },

        references          { public String pname() { return "references"; } },     // things that reference me
        stopped             { public String pname() { return "stopped"; } },
        
        last_ping           { 
            public String pname() { return "last-ping"; } 
            public String columnName() { return "last_ping"; } 
        },
        last_ping_readable  { 
            public String pname() { return "last-ping-readable"; } 
            public String columnName() { return "last_ping_readable"; } 
        },           
        last_runnable       { 
            public String pname() { return "last-runnable"; } 
            public String columnName() { return "last_runnable"; } 
        },
        last_runnable_readable { 
            public String pname() { return "last-runnable-readable"; } 
            public String columnName() { return "last_runnable_readable"; } 
        },
        work_instances      { 
            public String pname() { return "work-instances"; } 
            public String columnName() { return "work_instances"; } 
        },
        registration_date   { 
            public String pname() { return "registration-date"; } 
            public String columnName() { return "registration_date"; } 
        },
        registration_date_millis { 
            public String pname() { return "registration-date-millis"; } 
            public String columnName() { return "registration_date_millis"; } 
        },

        ;

        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }

     };
	
    // 
    // IMPORTANT IMPORTANT IMPORTANT 
    //
    // As of now, 2015/08/17, there is no support for service registration history in this class.
    // There never has been direct DUCC support for this other than keeping the old registrations
    // around.  Until there is a use-case and demand it's not clear what the interfaces would be
    // like.
    //
    // We may have to prune, or look at specific history files, but that can be done with direct database
    // operations.  If we need more, well add it later.
    //
    // The only nod to history we make here is the ability to move a registration and its meta from 'live' to
    // 'history' state.
    // IMPORTANT IMPORTANT IMPORTANT
    
     // Never used except in test.  Removing them pending complaints
	//public List<Long> getSvcList()  throws Exception;                               // list of registered services
	//public List<Long> getMetaList() throws Exception;                               // not used ?
	
    public enum AccessMode { RW, RO, NONE };
    
    public void setAccessMode(AccessMode accessMode);
    public AccessMode getAccessMode();
     
	public StateServicesDirectory getStateServicesDirectory() throws Exception;    // all the registy in one blow

    public boolean storeProperties (DuccId serviceId, Properties svc, Properties meta) throws Exception;   // save svc and meta in a transaction
    public boolean updateJobProperties (DuccId serviceId, Properties props)            throws Exception;   // update just job props
    public boolean updateMetaProperties(DuccId serviceId, Properties props)            throws Exception;   // update just metaprops
    public boolean moveToHistory(DuccId serviceId, Properties svc, Properties meta)    throws Exception;
    
    public void shutdown()                 throws Exception;    
    public boolean init(DuccLogger logger) throws Exception;
}
