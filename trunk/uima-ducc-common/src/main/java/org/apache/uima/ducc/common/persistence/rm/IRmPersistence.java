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

package org.apache.uima.ducc.common.persistence.rm;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public interface IRmPersistence
{
    /**
     * Establish a logger and anything else the persistence may need.
     *
     * @param logger This is the logger to be used.  It is usually
     *        the same logger as the client of persistence, e.g.
     *        org.apache.uima.ducc.rm.  The implementor is required
     *        to adjust itself to use this logger to insure 
     *        messages are logged into the right log.
     */
    public void init(DuccLogger logger) throws Exception;

    /**
     * When RM performs its configuration it must call this to clear the db of existing
     * nodes.  As nodes rejoin they'll be added back.  This is consistent with the RM's
     * internal management, which also drops it's structures and rebuilds them on (re)configuration.
     */
    public void clear() throws Exception;

    /**
     * Set multiple properties in one swell foop.
     *
     * @param id This is the primary key, the machine name;
     * @param properties These are the props, must be presented in the form of (String, Object) ...
     */
    public void setNodeProperties(String id, Object... properties) throws Exception;

    /**
     * Set a property on an object.  If the property cannot be set the action
     * is rolled back an the underlying store is unchanged.
     *
     * @param id This is the primary key and is usually the name of a host.
     * @param key This is the property key.
     * @param value This is the value to set.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. An
     *         exception causes the action to be rolled back.
     */
    public void setNodeProperty(String id, RmNodes key, Object value) throws Exception;

    
    /**
     * Write full information about a mach9ne into the DB.  We assume the machine
     * does not exist but in case it does, it is fully deleted first, and then
     * re-saved. If the recored cannot be saved the action
     * is rolled back an the underlying store is unchanged.
     *
     * @param id This is the primary key and is usually the name of a host.
     * @param props This is the full set of properties to be set.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. An
     *         exception causes the action to be rolled back.
     *
     * @return The db id of the created machine.
     */
    public void createMachine(String id, Map<RmNodes, Object> props) throws Exception;

    /**
     * Assign a share to this machine.
     * @param id The node name
     * @param jobid The duccid of the job owning the new shoare
     * @param shareid The DuccId of the new share.
     * @param quantum The scheduling quantum in GB used for this share.
     */
    public void addAssignment(String id, DuccId jobid, IDbShare share, int quantum, String jobtype) throws Exception;

    /**
     * Remove a share from the machine.
     * @param id The node name
     * @param jobid The duccid of the job owning the new shoare
     * @param shareid The DuccId of the new share.
     */
    public void removeAssignment(String id, DuccId jobid, IDbShare share) throws Exception;

    /**
     * Update a share definition to show it is non-preemptable.
     * @param node The node where the share resides.
     * @param shareId The (RM-assigned) DuccId of the share.
     * @param jobId The OR-assigned DuccId of the job.
     * @param val True if it is non-preemptable, false otherwise.
     *
     * NOTE: The triple (node, shareid, jobid) form the primary key to find the share in the DB
     */
    public void setFixed(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;
    /**
     * Update a share definition to show it is non-preemptable.
     * @param node The node where the share resides.
     * @param shareId The (RM-assigned) DuccId of the share.
     * @param jobId The OR-assigned DuccId of the job.
     * @param val True if it is non-preemptable.  RM will never set this false.
     *
     * NOTE: The triple (node, shareid, jobid) form the primary key to find the share in the DB
     */

    public void setPurged(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;

    /**
     * Update a share definition to show it is evicted aka preempted by RM.
     * @param node The node where the share resides.
     * @param shareId The (RM-assigned) DuccId of the share.
     * @param jobId The OR-assigned DuccId of the job.
     * @param val True if it is evicted.  Once evicted it can not be un-evicted so RM never sets this false.
     *
     * NOTE: The triple (node, shareid, jobid) form the primary key to find the share in the DB
     */
    public void setEvicted(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;

    /**
     * Update current information about the share from the current OR publication.
     * @param node The node where the share resides.
     * @param shareId The (RM-assigned) DuccId of the share.
     * @param jobId The OR-assigned DuccId of the job.
     * @param investment The "investment", i.e. amount of CPU expended by the process in the share so far.
     * @param state The OR-assigned state of the process in the share.
     * @param init_time The time the process spent in its initialization phase.
     * @param pid The *ix process id of the process in the share.
     *
     * NOTE: The triple (node, shareid, jobid) form the primary key to find the share in the DB
     */
    public void updateShare(String node, DuccId shareid, DuccId jobid, long investment, String state, long init_time, long pid) throws Exception;

    /**
     * Fetch a machine by its id.
     *
     * @param id This is the name of a specific machine and must exactly
     *           match the name of a machine in the DB.
     *
     * @return A properties object containing full details about the machine, or 
     *         null if no such machine exists.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. 
     */
    public Properties getMachine(String id) throws Exception;

    /**
     * Fetch all machines in the database.
     *
     * @return A map of map objects containing full details about the machines,
     *         keyed on machine name.  If there are no machines found in the db,
     *         an empty map is returned.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. 
     */
    public Map<String, Map<String, Object>> getAllMachines() throws Exception;

    /**
     * Fetch all shares in the database.
     *
     * @return A map of map objects containing full details about the shares.
     *         The key is node:jobid:shareid concatenated as a string, as it takes all of these
     *         to uniquely identify a share. If there are no shares found in the db,
     *         an empty map is returned.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. 
     */
    public Map<String, Map<String, Object>> getAllShares() throws Exception;
    
    /**
     * Fetch all the load records.  These are relatively short, one per job/reservation/etc
     * in the database, summarizing the resources they have vs the resources they want in
     * a perfect world.
     *
     * @return A list of map objects containing load information.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. 
     */
    public List<Map<String, Object>> getLoad() throws Exception;
    
    /**
     * A new job arrives (or is recovered after restart).
     */
    public void addJob(IDbJob j) throws Exception;

    /**
     * A job has left the system forever.
     */
    public void deleteJob(IDbJob j) throws Exception;

    /**
     * How many shares to I want from the scheduler?
     */
    public void updateDemand(IDbJob j) throws Exception;

    /**
     * Shutdown the connection to the DB;
     * 
     */
    public void close();
    
    enum RmNodes
        implements IDbProperty
    {
        TABLE_NAME {
            public String pname() { return "rmnodes"; }
            public Type type()  { return Type.String; }
            public boolean isPrivate() { return true;}
            public boolean isMeta() { return true;}
        },
        Name {
            public String pname() { return "name"; }
            public Type type()  { return Type.String; }
            public boolean isPrimaryKey() { return true;}
        },
        Memory {
            public String pname() { return "memory"; }
            public Type type()  { return Type.Integer; }
        },
        Nodepool {
            public String pname() { return "nodepool"; }
            public Type type()  { return Type.String; }
        },
        SharesLeft {
            public String pname() { return "shares_left"; }
            public Type type()  { return Type.Integer; }
        },
        Responsive{
            public String pname() { return "responsive"; }
            public Type type()  { return Type.Boolean; }
            public boolean isIndex() { return true; }
        },
        Online{
            public String pname() { return "online"; }
            public Type type()  { return Type.Boolean; }
            public boolean isIndex() { return true; }
        },
        Quiesced{
            public String pname() { return "quiesced"; }
            public Type type()  { return Type.Boolean; }
            public boolean isIndex() { return true; }
        },
        Reservable{
            public String pname() { return "reservable"; }
            public Type type()  { return Type.Boolean; }
            public boolean isIndex() { return true; }
        },
        Ip {
            public String pname() { return "ip"; }
            public Type type()  { return Type.String; }
        },
        Quantum {
            public String pname() { return "quantum"; }
            public Type type()  { return Type.Integer; }
        },
        Classes {
            public String pname() { return "classes"; }
            public Type type()  { return Type.String; }
        },
        ShareOrder {
            public String pname() { return "share_order"; }
            public Type type()  { return Type.Integer; }
        },
        Blacklisted {
            public String pname() { return "blacklisted"; }
            public Type type()  { return Type.Boolean; }
        },
        Heartbeats {
            public String pname() { return "heartbeats"; }
            public Type type()  { return Type.Integer; }
        },
        Assignments {
            public String pname() { return "assignments"; }
            public Type type()  { return Type.Integer; }
        },
        NPAssignments {
            public String pname() { return "np_assignments"; }
            public Type type()  { return Type.Integer; }
        },
        ;
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()    { return false; }
        public boolean isMeta()       { return false; }
        public String  columnName()   { return pname(); }
        public boolean isIndex()      { return false; }
    }

    enum RmShares
        implements IDbProperty
    {
        TABLE_NAME {
            public String pname() { return "rmshares"; }
            public Type type()  { return Type.String; }
            public boolean isPrivate() { return true;}
            public boolean isMeta() { return true;}
        },
        // share uniqueness is given by the rm duccid + jobid.  We add in the node to act as cassandra's cluster id
        // because the principal query at the moment is to find shares on a node.

        // the way cassandra works:  'node' will become the cluster key, and ducc_dbid, job_id the row key
        Node {
            public String pname() { return "node"; }
            public Type type()  { return Type.String; }
            public boolean isPrimaryKey() { return true; }
        },
        DuccDbid {
            public String pname() { return "ducc_dbid"; }
            public Type type()  { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        JobId {
            public String pname() { return "job_id"; }
            public Type type()  { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        Uuid {
            public String pname() { return "uuid"; }
            public Type type()  { return Type.UUID; }
        },
        ShareOrder {
            public String pname() { return "share_order"; }
            public Type type()  { return Type.Integer; }
        },
        Quantum {
            public String pname() { return "quantum"; }
            public Type type()  { return Type.Integer; }
        },
        InitTime {
            public String pname() { return "init_time"; }
            public Type type()  { return Type.Long; }
        },
        Evicted {
            public String pname() { return "evicted"; }
            public Type type()  { return Type.Boolean; }
        },
        Purged {
            public String pname() { return "purged"; }
            public Type type()  { return Type.Boolean; }
        },
        Fixed {
            public String pname() { return "fixed"; }
            public Type type()  { return Type.Boolean; }
        },
        Blacklisted {
            public String pname() { return "blacklisted"; }
            public Type type()  { return Type.Boolean; }
        },
        State {
            public String pname() { return "state"; }
            public Type type()  { return Type.String; }
        },
        Pid {
            public String pname() { return "pid"; }
            public Type type()  { return Type.Long; }
        },
        JobType {
            public String pname() { return "jobtype"; }
            public Type type()  { return Type.String; }
        },
        Investment {
            public String pname() { return "investment"; }
            public Type type()  { return Type.Long; }
        },
        ;

        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()    { return false; }
        public boolean isMeta()       { return false; }
        public String  columnName()   { return pname(); }
        public boolean isIndex()      { return false; }

    }

    /**
     * This table lists jobs in the system.
     */
    enum RmLoad
        implements IDbProperty
    {
        TABLE_NAME {
            public String pname() { return "rmload"; }
            public Type type()  { return Type.String; }
            public boolean isPrivate() { return true;}
            public boolean isMeta() { return true;}
        },

        Class {
            public String pname() { return "class"; }
            public Type type()  { return Type.String; }
        },

        JobId {
            public String pname() { return "job_id"; }
            public Type type()  { return Type.Long; }
            public boolean isPrimaryKey() { return true; }
        },

        User {
            public String pname() { return "user"; }
            public Type type()  { return Type.String; }
        },

        Memory {
            public String pname() { return "memory"; }
            public Type type()  { return Type.Integer; }
        },

        State {
            public String pname() { return "state"; }
            public Type type()  { return Type.String; }
        },

        Demand {
            public String pname() { return "demand"; }
            public Type type()  { return Type.Integer; }
        },

        Occupancy {
            public String pname() { return "occupancy"; }
            public Type type()  { return Type.Integer; }
        },

        JobType {
            public String pname() { return "jobtype"; }
            public Type type()  { return Type.String; }
        };

        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()    { return false; }
        public boolean isMeta()       { return false; }
        public String  columnName()   { return pname(); }
        public boolean isIndex()      { return false; }

    }
}


