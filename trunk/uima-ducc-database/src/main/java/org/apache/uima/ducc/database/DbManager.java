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

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

/**
 * Provide a common point for contacting the db, acquiring sessions/handles to it, managing the db,
 * closing, etc.
 */
public class DbManager
{
    private static String db_id = null;
    private static String db_pw = null;

    String[] dburls;
    DuccLogger logger;

    private Cluster cluster;            // only one
    private Session session;            // only one - it's thread safe and manages a connection pool

    
    public DbManager(String[] dburls, DuccLogger logger)
        throws Exception
    {
        this.dburls = dburls;
        this.logger = logger;
    }
    
    boolean checkForDatabase()
    	throws Exception
    {
        String methodName = "checkForDatabase";        
        logger.warn(methodName, null, "Not yet implemented.");
        return true;
    }

    public void drop()
        throws Exception
    {
        String methodName = "drop";
        logger.warn(methodName, null, "Drop is not implemented yet.");
    }

    public synchronized DbHandle open()
        throws Exception
    {
        if ( session == null ) {
            session = cluster.connect();
            session.execute(new SimpleStatement("USE " + DbCreate.DUCC_KEYSPACE));
        }

        return new DbHandle(this);
    }

	public synchronized void init()
    	throws Exception
    {
        String methodName = "init";

        if ( cluster != null ) return;        // already initialized
        dbPassword();                         // sets some private static login stuff.
                                              // will throw sometims, so we can assume
                                              // we're allowed to continue if control passes down.

        PlainTextAuthProvider auth = new PlainTextAuthProvider(db_id, db_pw); // throws if no good

        ReconnectionPolicy rp = new ConstantReconnectionPolicy(10000);  // if we lose connection, keep trying every 10 seconds
        cluster = Cluster.builder()
            .withAuthProvider(auth)
            .addContactPoints(dburls)
            .withReconnectionPolicy(rp)
            .build();

        Metadata metadata = cluster.getMetadata();
        logger.info(methodName, null, "Connected to cluster:", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.info(methodName, null, "Datatacenter:", host.getDatacenter(), "Host:", host.getAddress(), "Rack:", host.getRack());
        }
    }

    public synchronized void shutdown()
    {
    	String methodName = "closeDatabase";
        logger.info(methodName, null, "Closing the database.");
        if ( cluster != null ) cluster.close();        
        cluster = null;
        session = null;
    }

    PreparedStatement prepare(String cql)
    {
        return session.prepare(cql);
    }

    void truncate(String table)
        throws Exception
    {
        execute("TRUNCATE " + table);
    }

    String truncateText(String s)
    {
        String ret = s;
        if ( ret.length() > 200 ) {
            ret = s.substring(0, 200) + " ... ";
        }
        return ret;
    }

    ResultSet execute(String cql)
    {
    	String methodName = "execute";
        if ( logger.isDebug() ) {
            logger.debug(methodName, null, "EXECUTE CQL:", cql);
        } else {
            logger.trace(methodName, null, "EXECUTE CQL:", truncateText(cql));
        }
        return session.execute(cql);
    }

    ResultSet execute(BoundStatement s)
    {
        return session.execute(s);
    }

    ResultSet execute(SimpleStatement s)
    {
    	String methodName = "execute";
        logger.trace(methodName, null, "EXECUTE STATEMENT:", truncateText(s.getQueryString()));
        return session.execute(s);
    }

    static void dbPassword()
    	throws Exception
    {
        File f = new File(System.getProperty("DUCC_HOME") + "/resources.private/" + DbCreate.PASSWORD_FILE);
        // If i can read the file that's supposed to have the super user password I'll do so.  If not, or
        // if there's no password there, tough luck Charlie.
        db_id = "ducc";
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(f);
        props.load(fis);
        fis.close();

        db_pw = props.getProperty(DbCreate.PASSWORD_KEY);
        if ( db_pw == null ) {
            throw new IllegalStateException("Cannot acquire the database password.");
        }
    }


    public static void main(String[] args)
    {
    }

}
