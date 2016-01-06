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
package org.apache.uima.ducc.transport.event.common.history;

import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;


public interface IHistoryPersistenceManager 
{
	public void                       saveJob(IDuccWorkJob duccWorkJob) throws Exception;
	public IDuccWorkJob               restoreJob(long friendly_id)      throws Exception;
	public List<IDuccWorkJob>         restoreJobs(long max)             throws Exception;
	
	public void                       saveReservation(IDuccWorkReservation reservation) throws Exception;
	public IDuccWorkReservation       restoreReservation(long friendly_id)              throws Exception;
	public List<IDuccWorkReservation> restoreReservations(long max)                     throws Exception;

	public void                       saveService(IDuccWorkService duccWorkService) throws Exception;
	public IDuccWorkService           restoreService(long friendly_id)              throws Exception;
	public List<IDuccWorkService>     restoreServices(long max)                     throws Exception;

    public boolean checkpoint(DuccWorkMap work, Map<DuccId, DuccId> processToJob)   throws Exception;
    public Pair<DuccWorkMap, Map<DuccId, DuccId>>  restore()                        throws Exception;

    /**
     * Establish a logger and anything else the persistence may need.
     *
     * @param logger This is the logger to be used.  It is usually
     *        the same logger as the client of persistence, e.g.
     *        org.apache.uima.ducc.rm.  The implementor is required
     *        to adjust itself to use this logger to insure 
     *        messages are logged into the right log.
     */
    public boolean init(DuccLogger logger) throws Exception;

    public enum OrWorkProps    // properties for the OR work map 
        implements IDbProperty
    {
    	JOB_HISTORY_TABLE {
            public String pname()      { return "job_history"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},

    	RESERVATION_HISTORY_TABLE {
            public String pname()      { return "res_history"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},

    	SERVICE_HISTORY_TABLE {
            public String pname()      { return "svc_history"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},


        // The order of the primary keys is important here as the Db assigns semantics to the first key in a compound PK
        type {
            public String pname()         { return "type"; }     // "job", "reservation", "service", ...
            public boolean isPrimaryKey() { return true; }
        },

        ducc_dbid {
            public String pname()         { return "ducc_dbid"; }
            public Type type()            { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
            public boolean isIndex()      { return true; }
        },

        history {
            public String pname()         { return "history"; }        // to the future, is this a history or ckpt item?
            public Type type()            { return Type.Boolean; }
            public boolean isIndex()      { return true; }
        },

        work {
            public String pname() { return "work"; };
            public Type type()    { return Type.Blob; }
        },

        ;
        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }

     };

    public enum OrCkptProps    // properties for the OR checkpoint
        implements IDbProperty
    {
        CKPT_TABLE {
            public String pname()      { return "orckpt"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},

        id {
            public String pname()      { return "id"; }
            public boolean isPrimaryKey() { return true; }
            public Type type()         { return Type.Integer; }
        },

        work {
            public String pname() { return "work"; };
        },

        p2jmap {
            public String pname() { return "p2jmap"; };
        },

        ;
        public Type type() { return Type.Blob; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() {return pname(); }
        public Type listType() { return Type.String; }

     };

    public enum OrProcessProps
        implements IDbProperty
    {
    	TABLE_NAME {
            public String pname()      { return "processes"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},


        // The order of the primary keys is important here as the Db assigns semantics to the first key in a compound PK
        job_id {
            public String pname()         { return "job_id"; }
            public Type type()            { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },
            
        ducc_pid {
            public String pname()         { return "ducc_pid"; }
            public Type type()            { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        host {
            public String pname()         { return "host"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

            
        user {
            public String pname()         { return "user"; }                
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        log {
            public String pname()         { return "log"; }                
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        sclass {
            public String pname()         { return "class"; }        
            public Type type()            { return Type.String; }
        },

        pid {
            public String pname()         { return "pid"; }        
            public Type type()            { return Type.Long; }
        },

        start {
            public String pname()         { return "start"; }        
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        stop {
            public String pname()         { return "stop"; }       
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        reason_scheduler {
            public String pname()         { return "reason_scheduler"; }        
            public Type type()            { return Type.String; }
        },

        reason_agent {
            public String pname()         { return "reason_agent"; }        
            public Type type()            { return Type.String; }
        },

        exit_code {
            public String pname()         { return "exit_code"; }       
            public Type type()            { return Type.Integer; }
        },

        deallocation_type {
            public String pname()         { return "deallocation_type"; }       
            public Type type()            { return Type.String; }
        },

        memory {
            public String pname()         { return "memory"; }       
            public Type type()            { return Type.Integer; }
        },

        cpu {
            public String pname() { return "cpu"; };
            public Type type()    { return Type.Long; }
        },

        swap_max {
            public String pname() { return "swap_max"; };
            public Type type()    { return Type.Long; }
        },

        state_scheduler {
            public String pname()         { return "state_scheduler"; }
            public Type type()            { return Type.String; }
        },

        state_agent {
            public String pname()         { return "state_agent"; }
            public Type type()            { return Type.String; }
        },


        run_time {
            public String pname() { return "run_time"; };
            public Type type()    { return Type.Long; }
        },

        initialized {
            public String pname() { return "initialized"; };
            public Type type()    { return Type.Boolean; }
        },

        init_time {
            public String pname() { return "init_time"; };
            public Type type()    { return Type.Long; }
        },

        type {
            public String pname()         { return "type"; }     // J, R, S, A
            public boolean isIndex()      { return true; }
        },

        major_faults {
            public String pname()         { return "major_faults"; }
            public Type type()            { return Type.Long; }
        },

        investment {
            public String pname()         { return "investment"; }
            public Type type()            { return Type.Long; }
        },

        gccount {
            public String pname()         { return "gc_count"; }
            public Type type()            { return Type.Long; }
        },

        gctime {
            public String pname()         { return "gc_time"; }
            public Type type()            { return Type.Long; }
        },

        jconsole {
            public String pname()         { return "jconsole"; }
            public Type type()            { return Type.Long; }
        },

        ;
        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }

     };

    // jobs - only history so far.  some fields not used in db but here for the sake of complete schema
    public enum OrJobProps
        implements IDbProperty
    {
    	TABLE_NAME {
            public String pname()      { return "jobs"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},


        // The order of the primary keys is important here as the Db assigns semantics to the first key in a compound PK
        user {
            public String pname()         { return "user"; }
            public Type type()            { return Type.String; }
            public boolean isPrimaryKey() { return true; }
        },

        jclass {
            public String pname()         { return "class"; }
            public Type type()            { return Type.String; }
            public boolean isPrimaryKey() { return true; }
        },

        ducc_dbid {
            public String pname()         { return "ducc_dbid"; }
            public Type type()            { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        submission_time {
            public String pname()         { return "submission_time"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        duration {
            public String pname()         { return "duration"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        memory {
            public String pname()         { return "memory"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        services {
            public String pname()         { return "services"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        processes {
            public String pname()         { return "processes"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        reason {
            public String pname()         { return "reason"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        init_fails {
            public String pname()         { return "init_fails"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        run_fails {
            public String pname()         { return "run_fails"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        errors {
            public String pname()         { return "errors"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        state {
            public String pname()         { return "state"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        pgin {
            public String pname()         { return "pgin"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        swap {
            public String pname()         { return "swap"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        total_wi {
            public String pname()         { return "total_wi"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        done_wi {
            public String pname()         { return "done_wi"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        dispatch {
            public String pname()         { return "dispatch"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        retries {
            public String pname()         { return "retries"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        preemptions {
            public String pname()         { return "preemptions"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        description {
            public String pname()         { return "description"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },
        ;
        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }

    };

    // reservations - only history so far.  some fields not used in db but here for the sake of complete schema
    public enum OrReservationProps
        implements IDbProperty
    {
    	TABLE_NAME {
            public String pname()      { return "reservations"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},


        // The order of the primary keys is important here as the Db assigns semantics to the first key in a compound PK
        user {
            public String pname()         { return "user"; }
            public Type type()            { return Type.String; }
            public boolean isPrimaryKey() { return true; }
        },

        jclass {
            public String pname()         { return "class"; }
            public Type type()            { return Type.String; }
            public boolean isPrimaryKey() { return true; }
        },

        ducc_dbid {
            public String pname()         { return "ducc_dbid"; }
            public Type type()            { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        submission_time {
            public String pname()         { return "submission_time"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        duration {
            public String pname()         { return "duration"; }
            public Type type()            { return Type.Long; }
            public boolean isIndex()      { return true; }
        },

        memory {
            public String pname()         { return "memory"; }
            public Type type()            { return Type.Integer; }
            public boolean isIndex()      { return true; }
        },

        reason {
            public String pname()         { return "reason"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        processes {
            public String pname()         { return "processes"; }
            public Type type()            { return Type.Integer; }
        },

        state {
            public String pname()         { return "state"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        type {
            public String pname()         { return "type"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },

        hosts {
            public String pname()         { return "hosts"; }
            public Type type()            { return Type.String; }
        },

        description {
            public String pname()         { return "description"; }
            public Type type()            { return Type.String; }
            public boolean isIndex()      { return true; }
        },
        ;
        public Type type() { return Type.String; }
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()  { return false; }
        public boolean isMeta()  { return false; }
        public boolean isIndex()  { return false; }
        public String columnName() { return pname(); }

    };

	//public void serviceSaveConditional(IDuccWorkService duccWorkService) throws Exception;
	// public void serviceSave(IDuccWorkService duccWorkService) throws Exception;
	//public IDuccWorkService serviceRestore(String fileName);
	//public IDuccWorkService serviceRestore(DuccId duccId);
	//public ArrayList<String> serviceList();
	//public ArrayList<IDuccWorkService> serviceRestore() throws IOException, ClassNotFoundException;
}
