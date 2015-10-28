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

import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.utils.DuccLogger;

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
    public void setProperties(String id, Object... properties) throws Exception;

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
    public void setProperty(String id, RmProperty key, Object value) throws Exception;

    
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
    public void createMachine(String id, Map<RmProperty, Object> props) throws Exception;

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
     * @return A map of properties objects containing full details about the machines,
     *         keyed on machine name.  If there are no machines found in the db,
     *         an empty map is returned.
     *
     * @throws Exception.  Anything that goes wrong throws.  Usually the
     *         throw will originate in the DB because of some DB issue. 
     */
    public Map<String, Properties> getAllMachines() throws Exception;


    enum RmProperty
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
        Responsive{
            public String pname() { return "responsive"; }
            public Type type()  { return Type.Boolean; }
        },
        Online{
            public String pname() { return "online"; }
            public Type type()  { return Type.Boolean; }
        },
        Ip {
            public String pname() { return "ip"; }
            public Type type()  { return Type.String; }
        },
        Nodepool {
            public String pname() { return "nodepool"; }
            public Type type()  { return Type.String; }
        },
        Quantum {
            public String pname() { return "quantum"; }
            public Type type()  { return Type.Integer; }
        },
        Memory {
            public String pname() { return "memory"; }
            public Type type()  { return Type.Integer; }
        },
        ShareOrder {
            public String pname() { return "share_order"; }
            public Type type()  { return Type.Integer; }
        },
        Shares{
        	public String pname() { return "shares"; }
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
        SharesLeft {
            public String pname() { return "shares_left"; }
            public Type type()  { return Type.Integer; }
        },
        Assignments {
            public String pname() { return "assignments"; }
            public Type type()  { return Type.Integer; }
        },
        ;
        public boolean isPrimaryKey() { return false; }
        public boolean isPrivate()    { return false; }
        public boolean isMeta()       { return false; }
        public String columnName()     { return pname(); }
    }
}
