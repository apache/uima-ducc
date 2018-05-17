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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

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
    String[] db_host_list;
    String adminid = null;
    String adminpw = null;
    boolean useNewPw = false;

    private Cluster cluster;
    private Session session = null;


    DbCreate(String db_host_list, DuccLogger logger, String adminid, String adminpw)
    {
        this.db_host_list = toArray(db_host_list);
        this.logger = logger;
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    DbCreate(String db_host_list, String adminid, String adminpw)
    {
        this.db_host_list = toArray(db_host_list);
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    private String toString(String[] array) {
    	StringBuffer sb = new StringBuffer();
    	if(array != null) {
    		for(String item : array) {
    			sb.append(item);
    			sb.append(" ");
    		}
    	}
    	return sb.toString().trim();
    }
    
    private String[] toArray(String stringList) {
    	String methodName = "toArray";
    	String[] retVal = null;
    	if(stringList != null) {
    		retVal = stringList.split("\\s+");
        	debug(methodName, "list size =  " + retVal.length);  
            for(String item : retVal) {
            	debug(methodName, item);
            }
    	}
    	else {
    		debug(methodName, "null");
    	}
    	
    	return retVal;
    }
    
    void close()
    {
        if ( cluster != null ) cluster.close();
        session = null;
        cluster = null;
    }

    private void recommendation() {
    	doLog("Check cassandra.yaml for the following entries:");
    	doLog("> authenticator: PasswordAuthenticator");
    	doLog("> authorizer: org.apache.cassandra.auth.CassandraAuthorizer");
    }
    
    private void show(NoHostAvailableException e) {
    	String location = "show";
    	DuccId jobid = null;
    	if(logger != null) {
    		logger.debug(location, jobid, e);
    		Map<InetSocketAddress, Throwable> map = e.getErrors();
            for(Entry<InetSocketAddress, Throwable> entry : map.entrySet()) {
            	Throwable t = entry.getValue();
            	logger.debug(location, jobid, t);
            }
    	}
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
        doLog("database location(s): "+toString(db_host_list));

        for ( int i = 0; i < RETRY; i++ ) {
            try {
                // First time, we nuke the default id / pw and install our own.
                AuthProvider auth = new PlainTextAuthProvider("cassandra", "cassandra");
                cluster = Cluster.builder()
                    .withAuthProvider(auth)
                    .addContactPoints(db_host_list)
                    .build();
                    
                session = cluster.connect();
                session.execute("CREATE USER IF NOT EXISTS " + adminid + " with password '" + adminpw + "' SUPERUSER");
                cluster.close();
                doLog(methodName, "Created database super user " + adminid);                    
                    
                auth = new PlainTextAuthProvider(adminid, adminpw);
                cluster = Cluster.builder()
                    .withAuthProvider(auth)
                    .addContactPoints(db_host_list)
                    .build();
                session = cluster.connect();
                    
                String uglypw = UUID.randomUUID().toString();
                session.execute("ALTER USER cassandra  with password '" + uglypw + "' NOSUPERUSER");
                doLog(methodName, "Changed default database super user's password and revoked its superuser authority.");
                doLog(methodName, "From this point, this DB can only be accessed in super user mode by user 'ducc'");
                break;
            } catch ( NoHostAvailableException e ) {
                doLog("Waiting for database to boot ...");
                show(e);
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
                        .addContactPoints(db_host_list)
                        .build();
                    session = cluster.connect();                    
                    // if this works we assume the DB user base is ok and continue
                    break;         // no crash, we're outta here
                } catch ( Exception ee ) {
                    doLog(methodName, "Authorization fails with both the default userid/password and the new userid/password.");
                    doLog(methodName, "Retrying, as first-time database may take a few moments to initialize.");
                }
                session = null;
                cluster = null;
            } catch ( Exception e ) {
                doLog("Unknown problem contacting database.");
                recommendation();
                session = null;
                cluster = null;
                e.printStackTrace();
                return false;
            } 
            Thread.sleep(3000);
        }
            
        if ( cluster == null ) {
            doLog(methodName, "Excessive retries.  Database may not be initialized.");
            recommendation();
            return false;
        }
        Metadata metadata = cluster.getMetadata();
        doLog(methodName, "Connected to cluster:", metadata.getClusterName());
        
        for ( Host host : metadata.getAllHosts() ) {
            doLog(methodName, "Datatacenter:", host.getDatacenter(), "Host:", host.getAddress(), "Rack:", host.getRack());
        } 
        return true;
    }

    private boolean flag_debug = false;
    
    void debug(String methodName, Object ... msg) {
    	if(flag_debug) {
    		doLog(methodName, msg);
    	}
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

    	String guest_pw = adminpw;
    	
        // A 'keyspace' is what we usually think of as a database.
        session.execute("CREATE KEYSPACE IF NOT EXISTS ducc WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("CREATE USER IF NOT EXISTS guest  WITH PASSWORD '"+guest_pw+"' NOSUPERUSER");
        session.execute("GRANT SELECT ON KEYSPACE ducc TO guest");
        session.execute("REVOKE SELECT ON KEYSPACE system FROM guest");
        session.execute("REVOKE SELECT ON KEYSPACE system_auth FROM guest");
        session.execute("REVOKE SELECT ON KEYSPACE system_traces FROM guest");
        doLog(methodName, "Created database user 'guest' with SELECT priveleges on DUCC tables.");
                    
        session.execute("USE " + DUCC_KEYSPACE);

        try {
            List<SimpleStatement>rmSchema = RmStatePersistence.mkSchema();
            for ( SimpleStatement s : rmSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }

            List<SimpleStatement>smSchema = StateServicesDb.mkSchema();
            for ( SimpleStatement s : smSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }

            List<SimpleStatement>orSchema = HistoryManagerDb.mkSchema();
            for ( SimpleStatement s : orSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }
            
            /*
             * The following table for specifications is created by Orchestrator
             * at boot time, if not already present.  This additional table was 
             * not part of the DB in versions DUCC 2.2.1 and earlier.  In this
             * fashion, both migrated systems as well as fresh installs are able
             * to utilize this table without the need for invocation of 
             * ducc_post_install.  The commented-out code below is thus for
             * documentation purposes only.
             * 
            List<SimpleStatement>specificationsSchema = DbDuccWorks.mkSchema();
            for ( SimpleStatement s : specificationsSchema ) {
                doLog(methodName, "EXECUTE STATEMENT:", s.toString());
                session.execute(s);
            }
			*/
            
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
