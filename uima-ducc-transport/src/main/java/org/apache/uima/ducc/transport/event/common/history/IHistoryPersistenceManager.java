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
    	JOB_TABLE {
            public String pname()      { return "job_history"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},

    	RESERVATION_TABLE {
            public String pname()      { return "res_history"; } 
            public boolean isPrivate() { return true; }    		
            public boolean isMeta()    { return true; }    		
    	},

    	SERVICE_TABLE {
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

	//public void serviceSaveConditional(IDuccWorkService duccWorkService) throws Exception;
	// public void serviceSave(IDuccWorkService duccWorkService) throws Exception;
	//public IDuccWorkService serviceRestore(String fileName);
	//public IDuccWorkService serviceRestore(DuccId duccId);
	//public ArrayList<String> serviceList();
	//public ArrayList<IDuccWorkService> serviceRestore() throws IOException, ClassNotFoundException;
}
