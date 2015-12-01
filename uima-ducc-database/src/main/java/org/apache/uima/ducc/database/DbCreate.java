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

import java.util.List;
import java.util.UUID;

import org.apache.uima.ducc.common.utils.DuccLogger;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class DbCreate
{
    static final String DUCC_KEYSPACE = "ducc";
    static final String PASSWORD_KEY  = "db_password";
    static final String PASSWORD_FILE = "ducc.private.properties";
    int RETRY = 10;

    DuccLogger logger = null;
    String dburl;
    String adminid = null;
    String adminpw = null;
    boolean useNewPw = false;

    private Cluster cluster;
    private Session session = null;


    DbCreate(String dburl, DuccLogger logger, String adminid, String adminpw)
    {
        this.dburl = dburl;
        this.logger = logger;
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    DbCreate(String dburl, String adminid, String adminpw)
    {
        this.dburl = dburl;
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    void close()
    {
        if ( cluster != null ) cluster.close();
        session = null;
        cluster = null;
    }

    boolean connect()
        throws Exception
    {
        String methodName = "connect";

        String dh = System.getProperty("DUCC_HOME");
        if ( dh == null ) {
            throw new IllegalArgumentException("DUCC_HOME must be set as a system property: -DDUCC_HOME=whatever");
        }

        // If we're here, we must first of all get rid of the cassandra su and set up our own


        for ( int i = 0; i < RETRY; i++ ) {
            try {
                // First time, we nuke the default id / pw and install our own.
                AuthProvider auth = new PlainTextAuthProvider("cassandra", "cassandra");
                cluster = Cluster.builder()
                    .withAuthProvider(auth)
                    .addContactPoint(dburl)
                    .build();
                    
                session = cluster.connect();
                session.execute("CREATE USER IF NOT EXISTS " + adminid + " with password '" + adminpw + "' SUPERUSER");
                cluster.close();
                doLog(methodName, "Created user " + adminid);                    
                    
                auth = new PlainTextAuthProvider(adminid, adminpw);
                cluster = Cluster.builder()
                    .withAuthProvider(auth)
                    .addContactPoint(dburl)
                    .build();
                session = cluster.connect();
                    
                String uglypw = UUID.randomUUID().toString();
                session.execute("ALTER USER cassandra  with password '" + uglypw + "' NOSUPERUSER");
                doLog(methodName, "Changed default super user's password and revoked its superuser authority.");
                doLog(methodName, "From this point, this DB can only be accessed in super user mode by user 'ducc'");
                break;
            } catch ( NoHostAvailableException e ) {
                doLog("Waiting for database to boot ...");
                session = null;
                cluster = null;
            } catch ( AuthenticationException e ) {
                // The default userid/pw failed, so we try again with the user-supplied one
                RETRY += i;         // we'll extend the retry for a bit in case db took a while to start
                doLog(methodName, "Initial DB connection failed with AuthorizationException. Retrying database connection with your supplied userid and password.");
                try {
                    AuthProvider auth = new PlainTextAuthProvider(adminid, adminpw);
                    cluster = Cluster.builder()
                        .withAuthProvider(auth)
                        .addContactPoint(dburl)
                        .build();
                    session = cluster.connect();                    
                    // if this works we assume the DB user base is ok and continue
                    break;         // no crash, we're outta here
                } catch ( Exception ee ) {
                    doLog(methodName, "Authorization fails with both the default userid/password and the new userid/password.");
                    doLog(methodName, "Retrhying, as first-time database may take a few moments to initialize.");
                }
                session = null;
                cluster = null;
            } catch ( Exception e ) {
                doLog("Unknown problem contacting database.");
                session = null;
                cluster = null;
                e.printStackTrace();
                return false;
            } 
            Thread.sleep(3000);
        }
            
        if ( cluster == null ) {
            doLog(methodName, "Excessive retries.  Database may not be initialized.");
            return false;
        }
        Metadata metadata = cluster.getMetadata();
        doLog(methodName, "Connected to cluster:", metadata.getClusterName());
        
        for ( Host host : metadata.getAllHosts() ) {
            doLog(methodName, "Datatacenter:", host.getDatacenter(), "Host:", host.getAddress(), "Rack:", host.getRack());
        } 
        return true;
    }

    void doLog(String methodName, Object ... msg)
    {        
        if ( logger == null ) {

            StringBuffer buf = new StringBuffer(methodName);
            for ( Object o : msg ) {
                buf.append(" ");
                if ( o == null ) {
                    buf.append("<null>");
                } else {
                    buf.append(o.toString());
                }
            }            
            System.out.println(buf);
        } else {
            logger.info(methodName, null, msg);
            return;
        }

    }

    // String mkTableCreate(String tableName, String[] fields)
    // {
    //     int max = fields.length - 1;
    //     int current = 0;
    //     StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
    //     buf.append(tableName);
    //     buf.append(" (");
    //     for (String s : fields) {
    //         buf.append(s);
    //         if ( current++ < max) buf.append(", ");
    //     }
    //     buf.append(") WITH CLUSTERING ORDER BY (ducc_dbid desc)");
    //     return buf.toString();                   
    // }

    void createSchema()
    {
    	String methodName = "createSchema";

        // A 'keyspace' is what we usually think of as a database.
        session.execute("CREATE KEYSPACE IF NOT EXISTS ducc WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("CREATE USER IF NOT EXISTS guest  WITH PASSWORD 'guest' NOSUPERUSER");
        session.execute("GRANT SELECT ON KEYSPACE ducc TO guest");
        session.execute("REVOKE SELECT ON KEYSPACE system FROM guest");
        session.execute("REVOKE SELECT ON KEYSPACE system_auth FROM guest");
        session.execute("REVOKE SELECT ON KEYSPACE system_traces FROM guest");
        doLog(methodName, "Created user 'guest' with SELECT priveleges on DUCC tables.");
                    
        session.execute("USE " + DUCC_KEYSPACE);

        try {
            List<SimpleStatement>rmSchema = RmStatePersistence.mkSchema();
            for ( SimpleStatement s : rmSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }

            //
            //        String[] rmSchema = RmStatePersistence.mkSchemaItems();
            //        String cql = DbUtil.mkTableCreate("ducc.rmnodes", rmSchema);
            //        doLog(methodName, "CQL:", cql);
            //        session.execute(cql);

            List<SimpleStatement>smSchema = StateServicesDb.mkSchema();
            for ( SimpleStatement s : smSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }


//            String[] smSchemaReg = StateServicesDb.mkSchemaForReg();
//            cql = DbUtil.mkTableCreate("ducc.smreg", smSchemaReg);
//            doLog(methodName, "CQL:", cql);
//            session.execute(cql);
//            cql = "CREATE INDEX IF NOT EXISTS ON ducc.smreg(active)";
//            session.execute(cql);
//
//            String[] smSchemaMeta = StateServicesDb.mkSchemaForMeta();
//            cql = DbUtil.mkTableCreate("ducc.smmeta", smSchemaMeta);
//            doLog(methodName, "CQL:", cql);
//            session.execute(cql);
//            cql = "CREATE INDEX IF NOT EXISTS ON ducc.smmeta(active)";
//            session.execute(cql);

            List<SimpleStatement>orSchema = HistoryManagerDb.mkSchema();
            for ( SimpleStatement s : orSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }

        } catch ( Exception e ) {
            doLog(methodName, "Cannot create schema:", e);
        }

    }

    /**
     * Create the database and initialize the schema.  This is intended to be called only from Main at
     * system startup, to insure all users of the db have a db when they start.
     */
    boolean createDatabase()
        throws Exception
    {
        //String methodName = "createDatabase";

        return true;
    }

    public static void main(String[] args)
    {
        if ( args.length != 3 ) {
            System.out.println("Usage: DbCreate database_url db_id db_pw");
            System.exit(1);
        }

        DbCreate client = null;
        try {
            client = new DbCreate(args[0], args[1], args[2]);
            if ( client.connect() ) {
                client.createSchema();
                client.close();
            } else {
                System.exit(1);
            }
        } catch ( Throwable e ) {
            System.out.println("Errors creating database");
            e.printStackTrace();
            System.exit(1);
        } 

        System.exit(0);
    }

}
