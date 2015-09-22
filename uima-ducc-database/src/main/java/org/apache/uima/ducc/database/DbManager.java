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

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class DbManager
{

    String dburl;
    DuccLogger logger;

    OrientGraphFactory  factory;

    Map<String, DbVertex> name_to_vertex = new HashMap<String, DbVertex>();
    Map<String, DbEdge>   name_to_edge   = new HashMap<String, DbEdge>();

    // private ODatabaseDocumentTx documentDb;

    public DbManager(String dburl, DuccLogger logger)
        throws Exception
    {
        this.dburl = dburl;
        this.logger = logger;

        for ( DbVertex o :  DbVertex.values() ) {
            name_to_vertex.put(o.pname(), o);
        }
        for ( DbEdge o :  DbEdge.values() ) {
            name_to_edge.put(o.pname(), o);
        }

    }

    DbVertex vertexType(String v)
    {
        return name_to_vertex.get(v);
    }

    DbEdge EdgeType(String e)
    {
        return name_to_edge.get(e);
    }

    
    boolean checkForDatabase()
    	throws Exception
    {
        String methodName = "checkForDatabase";        
        String pw = dbPassword();
        OServerAdmin admin = null;

        boolean ret = true;
        try {
            admin = new OServerAdmin(dburl);
            admin.connect("root", pw);               // connect to the server
                       
            if ( ! admin.existsDatabase("plocal") ) {
                logger.info(methodName, null, "Database " + dburl + " does not exist.");
                ret = false;
            }
        } finally {
            if ( admin != null) admin.close();
        }
        return ret;
    }

    public void drop()
        throws Exception
    {
    	OrientGraphNoTx graphDb = factory.getNoTx();        // the graph instance
        if ( graphDb == null ) {
            throw new IllegalStateException("Cannot allocate graph instance for " + dburl);
        }
        graphDb.drop();
    }

    public synchronized DbHandle open()
        throws Exception
    {
    	OrientGraph graphDb = factory.getTx();        // the graph instance
        if ( graphDb == null ) {
            throw new IllegalStateException("Cannot allocate graph instance for " + dburl);
        }
        
        return new DbHandle(this, graphDb);
    }


    public synchronized DbHandle openNoTx()
        throws Exception
    {
    	OrientGraphNoTx graphDb = factory.getNoTx();        // the graph instance
        if ( graphDb == null ) {
            throw new IllegalStateException("Cannot allocate graph instance for " + dburl);
        }
        
        return new DbHandle(this, graphDb);
    }


	public synchronized void init()
    	throws Exception
    {
        String methodName = "init";

        if ( factory != null ) return;        // already initialized

        if ( ! dburl.startsWith("plocal") ) {
            // make sure the server is up if it's not a plocal db
            if ( !checkForDatabase() ) {
                throw new IllegalStateException("Database does not exist and must be created:" + dburl);
            }
        }

        factory = new OrientGraphFactory(dburl);
        if ( factory == null ) {
            throw new IllegalStateException("Cannot create graph factory for " + dburl);
        } 
        logger.info(methodName, null, "Database is opened:", dburl);
        factory.setupPool(1,20);        
    }

    public synchronized void shutdown()
    {
    	String methodName = "closeDatabase";
        logger.info(methodName, null, "Closing the database.");
        if ( factory != null ) {
            // closes all pooled instances and stops the factory
            factory.close();
            factory = null;
        }        
    }


    static String dbPassword()
    	throws Exception
    {
        // logger.info(methodName, null, "Opening service database at: "  + dburl);
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(System.getProperty("DUCC_HOME") + "/resources.private/db_password");
        props.load(fis);
        fis.close();

        String pw = props.getProperty("db_password");
        if ( pw == null ) {
            throw new IllegalStateException("Cannot acquire the database password.");
        }
        return pw;
    }


    public static void main(String[] args)
    {
    }

}
