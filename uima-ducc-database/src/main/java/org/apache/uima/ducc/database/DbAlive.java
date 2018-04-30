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

import java.util.Collection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class DbAlive
{
    String db_host_list;
    String adminid = null;
    String adminpw = null;

    enum RC {
        OK {
            public String message() { return "OK"; }
            public int    rc()      { return 0; }
        },
        NOT_INITIALIZED {
            public String message() { return "DB Not Initialized"; }
            public int    rc()      { return 1; }
        },
        CANT_CONNECT {
            public String message() { return "DB Cannot Connect"; }
            public int    rc()      { return 2; }
        },
        NOT_AUTHORIZED {
            public String message() { return "DB Not Authorized"; }
            public int    rc()      { return 3; }
        },
        UNKNOWN {
            public String message() { return "DB Unknown Error"; }
            public int    rc()      { return 4; }
        },
        ;
    	public abstract String message();
    	public abstract int    rc();
    };

    private Cluster cluster;
    //private Session session;

    public DbAlive(String db_host_list, String adminid, String adminpw)
    {
        this.db_host_list = db_host_list;
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    public RC connect()
        throws Exception
    {
        RC ret = RC.OK;
        try {
            PlainTextAuthProvider auth = new PlainTextAuthProvider(adminid, adminpw);
            cluster = Cluster.builder()
                .withAuthProvider(auth)
                .addContactPoints(db_host_list.split("\\s+"))
                .build();

            Metadata metadata = cluster.getMetadata();
            System.out.println("Connected to cluster: " + metadata.getClusterName());
            KeyspaceMetadata  duccKs = metadata.getKeyspace(DbCreate.DUCC_KEYSPACE);
            if ( duccKs == null ) {
                System.out.println("DUCC keyspace not found.");
                ret = RC.NOT_INITIALIZED;
            } else {
                String tables = "";
                Collection<TableMetadata> tableset = duccKs.getTables();
                for ( TableMetadata tmd : tableset ) {
                    tables = tables + tmd.getName() + " ";
                }
                System.out.println("Tables found: " + tables);
            }

            for ( Host host : metadata.getAllHosts() ) {
                System.out.println(String.format("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack()));
            } 
        } catch ( NoHostAvailableException e ) {
            // If this never clears the db is either not started, or is broken and can't talk
            System.out.println("Waiting for DB to start ...");
            ret = RC.CANT_CONNECT;
        } catch ( AuthenticationException e ) {
            // If this never clears the db is not correctly configured
            System.out.println("Waiting for authentication ...");
            ret = RC.NOT_AUTHORIZED;
        } catch ( Exception e ) {
        	e.printStackTrace();
            ret = RC.UNKNOWN;
        } finally {
            if ( cluster != null ) cluster.close();
        }
        return ret;
    }

    public void close() {
        cluster.close();
    }

    static void usage()
    {
        System.out.println("Usage: DbAlive database_url id pw retry-count");
        System.exit(1);
    }

    public static void main(String[] args)
    {
        if ( args.length != 4 ) {
            usage();
        }

        int max = 0;
        try {
            max = Integer.parseInt(args[3]);                         // we'll wait up to 60 seconds: 20 x 3 seconds
        } catch ( NumberFormatException e ) {
            System.out.println("Retry count must be numeric.");
            System.exit(1);
        }

        DbAlive client = null;
        RC rc = RC.OK;
        try {
            client = new DbAlive(args[0], args[1], args[2]);
            for ( int i = 0; i < max; i++ ) {
                rc = client.connect();
                System.out.println(rc.message());
                if ( rc == RC.OK) {
                    break;
                } else {
                    try { Thread.sleep(3000); } catch ( Exception e ) {}
                }
            }
        } catch ( Throwable e ) {
            System.out.println("Errors contacting database");            
            e.printStackTrace();
            System.exit(rc.rc());
        } 
        System.exit(rc.rc());
    }

}
