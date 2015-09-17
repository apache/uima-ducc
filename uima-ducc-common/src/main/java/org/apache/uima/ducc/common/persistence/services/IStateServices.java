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

import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public interface IStateServices {

	public static String svc_reg_dir = IDuccEnv.DUCC_STATE_SVCREG_DIR;
	public static String svc_hist_dir = IDuccEnv.DUCC_HISTORY_SVCREG_DIR;


    public static final String archive_key = "is_archived";
    public static final String archive_flag = "true";

	public static final String svc = "svc";
	public static final String meta = "meta";

    public enum SvcProps {
        endpoint            { public String pname() { return "endpoint"; } },
        instances           { public String pname() { return "instances"; } },
        autostart           { public String pname() { return "autostart"; } },
        reference           { public String pname() { return "reference"; } },
        enabled             { public String pname()   { return "enabled"; } },

        disable_reason      { public String pname() { return "disable_reason"; } },

        implementors        { public String pname() { return "implementors"; } },
        numeric_id          { public String pname() { return "numeric_id"; } },
        ping_active         { public String pname() { return "ping_active"; } },
        ping_only           { public String pname() { return "ping_only"; } },

        service_alive       { public String pname() { return "service_alive"; } },
        service_class       { public String pname() { return "service_class"; } },
        service_dependency  { public String pname() { return "service_dependency"; } },
        service_healthy     { public String pname() { return "service_healthy"; } },
        service_state       { public String pname() { return "service_state"; } },
        last_use            { public String pname() { return "last_use"; } },
        last_use_readable   { public String pname() { return "last_use_readable"; } },
        service_statistics  { public String pname() { return "service_statistics"; } },
        service_type        { public String pname() { return "service_type"; } },
        submit_error        { public String pname() { return "submit_error"; } },
        user                { public String pname() { return "user"; } },

        references          { public String pname() { return "references"; } },
        stopped             { public String pname() { return "stopped"; } },
        
        scheduling_class    { public String pname() { return "scheduling_class"; } },
        process_memory_size { public String pname() { return "process_memory_size"; } },
        description         { public String pname() { return "description"; } },
        log_directory       { public String pname() { return "log_directory"; } },
        process_executable  { public String pname() { return "process_executable"; } },

        last_ping           { public String pname() { return "last_ping"; } },
        last_ping_readable  { public String pname() { return "last_ping_readable"; } },           
        last_runnable       { public String pname() { return "last_runnable"; } },
        last_runnable_readable { public String pname() { return "last_runnable_readable"; } },
        work_instances      { public String pname() { return "work_instances"; } },
        registration_date   { public String pname() { return "registration_date"; } },

        instance_init_failures_limit { public String pname() { return "instance_init_failures_limit"; } },

        UIMA_AS             { public String pname() { return "UIMA-AS"; } },
        CUSTOM              { public String pname() { return "CUSTOM"; } },

        ;
        public abstract String pname();

    };

	//public static final String endpoint { public String pname() { return "endpoint"; } },
	//public static final String instances = "instances";
	//public static final String autostart = "autostart";

	//public static final String reference = "reference";
	//public static final String enabled = "enabled";
	//public static final String disable_reason = "disable-reason";
	//public static final String implementors = "implementors";
	//public static final String numeric_id = "numeric_id";
	//public static final String ping_active = "ping-active";
	//public static final String ping_only = "ping-only";
	//public static final String service_alive = "service-alive";
	//public static final String service_class = "service-class";
	//public static final String service_dependency = "service_dependency";
	//public static final String service_healthy = "service-healthy";
	//public static final String service_state = "service-state";
	//public static final String last_use = "last-use";
	//public static final String service_statistics = "service-statistics";
	//public static final String service_type = "service-type";
	//public static final String submit_error = "submit-error";
	//public static final String user = "user";
	
	//public static final String scheduling_class = "scheduling_class";
	//public static final String process_memory_size = "process_memory_size";
	//public static final String description = "description";
	//public static final String log_directory = "log_directory";
	//public static final String process_executable = "process_executable";
	
	//public static final String service_type_UIMA_AS = "UIMA-AS";
	//public static final String service_type_CUSTOM = "CUSTOM";
	
    // 
    // IMPORTANT IMPORTANT IMPORTANT
    // As of now, 2015/08/17, there is no support for service registration history.  There never has been in
    // this class and it's derivatives.  Until there is a use-case and demand it's not clear what the interfaces
    // would be like.
    //
    // We may have to prune, or look at specific history files, but that can be done with direct database
    // operations.  If we need more, well add it later.
    //
    // The only nod to history we make here is the ability to move a registration and its meta from 'live' to
    // 'history' state.
    // IMPORTANT IMPORTANT IMPORTANT
    
	public List<Long> getSvcList()  throws Exception;                               // list of registered services
	public List<Long> getMetaList() throws Exception;                               // not used ?
	
	public StateServicesDirectory getStateServicesDirectory() throws Exception;    // all the registy in one blow

    public boolean storeProperties (DuccId serviceId, Properties svc, Properties meta) throws Exception;   // save svc and meta in a transaction
    public boolean updateJobProperties (DuccId serviceId, Properties props)            throws Exception;   // update just job props
    public boolean updateMetaProperties(DuccId serviceId, Properties props)            throws Exception;   // update just metaprops
    public void    moveToHistory(DuccId serviceId, Properties svc, Properties meta)    throws Exception;
    
    public void shutdown()                 throws Exception;    
    public boolean init(DuccLogger logger) throws Exception;
}
