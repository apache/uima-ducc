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

import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class DbCreate
{
    String dburl;
    OServerAdmin admin;
    OrientGraphFactory  factory;

    public DbCreate(String dburl)
    {
        this.dburl = dburl;
    }

    void createEdgeType(OrientGraphNoTx g, DbEdge id)
    {
    	String s = id.pname();
        OrientEdgeType e = g.getEdgeType(s);
        if ( e == null ) {
            System.out.println("Create edge " + s);
            g.createEdgeType(s);
        }
    }

    void createVertexType(OrientGraphNoTx g, DbVertex id)
    {
    	String s = id.pname();
        OrientVertexType e = g.getVertexType(s);
        if ( e == null ) {
            System.out.println("Create vertex " + s);
            e = g.createVertexType(s);
            OProperty p = e.createProperty(DbConstants.DUCCID, OType.LONG);
            p.setMandatory(true);
        }
    }

    void createSchema()
    {
        OrientGraphNoTx g = factory.getNoTx();

        for ( DbVertex o :  DbVertex.values() ) {
            createVertexType(g, o);
        }
        for ( DbEdge o :  DbEdge.values() ) {
            createEdgeType(g, o);
        }
                
        g.shutdown();
    }

    /**
     * Create the database and initialize the schema.  This is intended to be called only from Main at
     * system startup, to insure all users of the db have a db when they start.
     */
    boolean createDatabase()
        throws Exception
    {
        String pw = DbManager.dbPassword();

        try {
            admin = new OServerAdmin(dburl);
            admin.connect("root", pw);               // connect to the server

            if ( ! admin.existsDatabase("plocal") ) {
                System.out.println("Database " + dburl + " does not exist, attempting to create it.");
                admin.createDatabase("graph", "plocal");
                
                if ( ! admin.existsDatabase() ) {
                    System.out.println("Cannot create database " + dburl);
                    return false;
                } 
                factory = new OrientGraphFactory(dburl);
                if ( factory == null ) {
                    System.out.println("Cannot create graph factory for " + dburl);
                    return false;
                }
                
                createSchema();
            }
        } finally {
            if ( admin != null   ) admin.close();
            if ( factory != null ) factory.close();                
        }
        return true;
    }

    public static void main(String[] args)
    {
        if ( args.length != 1 ) {
            System.out.println("Usage: DbCreate <database url>");
            System.exit(1);
        }
        try {
            DbCreate dbc = new DbCreate(args[0]);
            if ( ! dbc.createDatabase() ) {
                System.out.println("Could not create database or schema for " + args[0]);
                System.exit(1);
            }
        } catch ( Exception e ) {
            System.out.println("Errors creating database");
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

}
