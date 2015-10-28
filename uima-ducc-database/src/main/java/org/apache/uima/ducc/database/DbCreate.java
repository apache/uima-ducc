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

import org.apache.uima.ducc.common.utils.DuccLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class DbCreate
{
    DuccLogger logger = null;
    String dburl;
    String adminid = "root";
    String adminpw = null;

    private Cluster cluster;
    private Session session;

    public DbCreate(String dburl)
    {
        this.dburl = dburl;
    }


    public DbCreate(String dburl, DuccLogger logger)
    {
        this.dburl = dburl;
        this.logger = logger;
    }

    public DbCreate(String dburl, String adminid, String adminpw)
    {
        this.dburl = dburl;
        this.adminid = adminid;
        this.adminpw = adminpw;
    }

    public void connect()
    {
        String methodName = "connect";
        cluster = Cluster.builder()
            .addContactPoint(dburl)
            .build();

        Metadata metadata = cluster.getMetadata();
        doLog(methodName, "Connected to cluster: %s\n", metadata.getClusterName());
        
        for ( Host host : metadata.getAllHosts() ) {
            doLog(methodName, "Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        } 
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }
    
    public Session getSession()
    {
        return this.session;
    }

    public void doLog(String methodName, Object ... msg)
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

    String mkTableCreate(String tableName, String[] fields)
    {
        int max = fields.length - 1;
        int current = 0;
        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        buf.append(tableName);
        buf.append(" (");
        for (String s : fields) {
            buf.append(s);
            if ( current++ < max) buf.append(", ");
        }
        buf.append(") WITH CLUSTERING ORDER BY (ducc_dbid desc)");
        return buf.toString();                   
    }

    void createSchema()
    {
    	String methodName = "createSchema";

        // A 'keyspace' is what we usually think of as a database.
        session.execute("CREATE KEYSPACE IF NOT EXISTS ducc WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");

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
        if ( args.length != 1 ) {
            System.out.println("Usage: DbCreate <database url>");
            System.exit(1);
        }

        DbCreate client = null;
        try {
            client = new DbCreate(args[0]);
            client.connect();
            client.createSchema();
        } catch ( Throwable e ) {
            System.out.println("Errors creating database");
            e.printStackTrace();
            System.exit(1);
        } finally {
            if ( client != null ) client.close();
        }

        System.exit(0);
    }

}
