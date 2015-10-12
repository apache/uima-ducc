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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;
import org.apache.uima.ducc.database.DbConstants.Index;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class DbCreate
{
    DuccLogger logger = null;
    String dburl;
    String adminid = "root";
    String adminpw = null;
    OServerAdmin admin;
    OrientGraphFactory  factory;

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

    void createEdgeType(OrientGraphNoTx g, DbEdge id)
    {
        String methodName = "createEdgeType";
    	String s = id.pname();
        OrientEdgeType e = g.getEdgeType(s);
        if ( e == null ) {
            doLog(methodName, "Create edge", s);
            g.createEdgeType(s);
        }
    }

//    void createVertexType(OrientGraphNoTx g, DbVertex id)
//    {
//        String methodName = "createVertexType";
//    	String s = id.pname();
//        OrientVertexType e = g.getVertexType(s);
//        if ( e == null ) {
//            doLog(methodName, "Create vertex " + s);
//            e = g.createVertexType(s, DuccVertexBase.VBase.pname());
//        }
//    }

    void createVertices(OrientGraphNoTx g)
    {    
    	String methodName = "createVertices";
    
        for (DbVertex v : DbVertex.values()) {
            String s = v.pname();
            OrientVertexType vt = g.getVertexType(s);
            

            if ( vt == null ) {
                if ( v.parent() == null ) {
                    vt = g.createVertexType(s);
                    doLog(methodName, "Create vertex " + s);
                } else {
                    vt = g.createVertexType(s, v.parent().pname());
                    doLog(methodName, "Create vertex ", s, "subclass from", v.parent().pname());
                }
                
                Index[] ndx = v.indices();
                if ( ndx != null ) {
                    for ( Index i : ndx ) {                    
                        OProperty p = vt.createProperty(i.propname(), i.type());
                        p.setMandatory(true);
                        doLog(methodName, "Create property", i.propname(), "on", s);
                    }
                }
        
                //         String sql = "create index " + i.pname() + " on " + s + "(" + i.propname() + " by key) notunique";
                //         g.command(new OCommandSQL(sql)).execute();
                //         doLog(methodName, "(sql)Created index", i.pname(), "on class", s, "for", i.propname());                    
                //         //g.createKeyIndex(i.propname(), Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"));
                //         doLog(methodName, "(java)Created index on class Vertex for", i.propname());                    
                //     }
                // }

            }    
        }

        if ( true ) return;
        String sql = "create index D.VBASE_0 on VWork(ducc_dbid, ducc_dbcat) NOTUNIQUE";
        g.command(new OCommandSQL(sql)).execute();

        String sql1 = "create index D.VBASE_ID on VWork(ducc_dbid) NOTUNIQUE";
        g.command(new OCommandSQL(sql1)).execute();

        String sql2 = "create index D.VBASE_CAT on VWork(ducc_dbcat) NOTUNIQUE";
        g.command(new OCommandSQL(sql2)).execute();

        String sql3 = "create index D.NODE on VRmNode(ducc_dbnode) NOTUNIQUE";
        g.command(new OCommandSQL(sql3)).execute();

    }

    void createEdges(OrientGraphNoTx g)
    {
        String methodName = "createEdges";
        for (DbEdge e : DbEdge.values()) {
            String s = e.pname();
            OrientEdgeType et = g.getEdgeType(s);
            if ( et == null ) {
                if ( e.parent() == null ) {
                    et = g.createEdgeType(s);
                    doLog(methodName, "Create edge " + s);
                } else {
                    et = g.createEdgeType(s, e.parent().pname());
                    doLog(methodName, "Create edge ", s, "subclass from", e.parent().pname());
                }
            }    
        }        
    }
    
    void createSchema()
    {
        OrientGraphNoTx g = factory.getNoTx();

        try {
            createVertices(g);
            createEdges(g);
        } finally {        
            g.shutdown();
        }
    }

    // void createSchemax()
    // {
    // 	String methodName = "createSchema";
    //     OrientGraphNoTx g = factory.getNoTx();

    // 	String base =  DuccVertexBase.VBase.pname();
    //     OrientVertexType e = g.getVertexType(base);
    //     if ( e == null ) {
    //         doLog(methodName, "Create base vertex class " + base);
    //         e = g.createVertexType(base);
    //         OProperty p = e.createProperty(DbConstants.DUCCID, OType.LONG);
    //         p.setMandatory(true);
    //         OProperty p2 = e.createProperty(DbConstants.DUCC_DBCAT, OType.STRING);
    //         p2.setMandatory(true);

    //         String sql = "create index i_ducc_dbid on " + base + "(" + DbConstants.DUCCID + ") notunique";
    //         g.command(new OCommandSQL(sql)).execute();
    //         doLog(methodName, "(sql)Created index i_ducc_dbid on class " + base + " for " + DbConstants.DUCCID);

    //         sql = "create index i_ducc_dbcat on " + base + "(" + DbConstants.DUCC_DBCAT + ") notunique";
    //         g.command(new OCommandSQL(sql)).execute();
    //         doLog(methodName, "(sql)Created index i_ducc_dbcat on class " + base + " for " + DbConstants.DUCC_DBCAT);

    //     }
        
    //     for ( DbVertex o :  DbVertex.values() ) {
    //         createVertexType(g, o);
    //     }
    //     for ( DbEdge o :  DbEdge.values() ) {
    //         createEdgeType(g, o);
    //     }
                
    //     g.shutdown();
    // }

    boolean createPlocalDatabase()
        throws Exception
    {
        boolean ret = false;
        try {
            factory = new OrientGraphFactory(dburl, "admin", "admin");
            createSchema();
            ret = true;
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            factory.close();
        }
        return ret;
    }
    
    /**
     * Create the database and initialize the schema.  This is intended to be called only from Main at
     * system startup, to insure all users of the db have a db when they start.
     */
    boolean createDatabase()
        throws Exception
    {
        String methodName = "createDatabase";

        if ( adminpw == null ) {
            adminpw = DbManager.dbPassword();
        }

        try {
            admin = new OServerAdmin(dburl);
            admin.connect(adminid, adminpw);               // connect to the server

            if ( ! admin.existsDatabase("plocal") ) {
                doLog(methodName, "Database " + dburl + " does not exist, attempting to create it.");
                admin.createDatabase("graph", "plocal");
                
                if ( ! admin.existsDatabase() ) {
                    doLog(methodName, "Cannot create database " + dburl);
                    return false;
                } 
                factory = new OrientGraphFactory(dburl);
                if ( factory == null ) {
                    doLog(methodName, "Cannot create graph factory for " + dburl);
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
