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

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.DuccNode;
import org.apache.uima.ducc.common.IIdentity;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.IDuccId;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeployableConfiguration;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class DbHandle
{
    private DuccLogger logger = null;
    
    DbManager manager;
    public OrientBaseGraph graphDb;

    DbHandle(DbManager manager, OrientBaseGraph graphDb)
    {
        if ( DuccService.getDuccLogger() == null ) {
            // not running within a ducc service - just get a regular logger
            logger = DuccLogger.getLogger(DbHandle.class, "DB");
        } else {
            // running within a ducc service - get the component logger
            logger = DuccService.getDuccLogger(DbHandle.class.getName());
        }
        this.manager = manager;
        this.graphDb = graphDb;
    }


    public void close()
    {
        if ( graphDb == null ) return;
        graphDb.shutdown();
        graphDb = null;
    }
    
    /**
     * We use id + isHistory + vertex class = primary key
     * @param id The numeric duccid of the 'thing'
     * @param type the enum representing the database class of the object 
     * @param isHistory 'true' if we're searching history, 'false' otherwise
     */
    public boolean thingInDatabase(long id, DbVertex type, DbCategory category)
        throws Exception
    {
    	String methodName = "thingInDatabase";
        Iterable<Vertex> vs =  null;
        if ( category == DbCategory.Any ) {
            vs = select("SELECT count(*) FROM " + type.pname() + 
                                         " WHERE " + DbConstants.DUCCID + "="  + id);
        } else {
            vs = select("SELECT count(*) FROM " + type.pname() + 
                                         " WHERE " + DbConstants.DUCCID + "="  + id + 
                                         " AND " + DbConstants.DUCC_DBCAT + "='" + category.pname() + "'");
        }

        try {
            for ( Vertex v : vs ) {
                OrientVertex ov = (OrientVertex) v;
                ODocument doc = ov.getRecord();
                long did = doc.field("count");
                return (did > 0);
            }            
		} catch (Throwable e) {
			logger.error(methodName, null, e);			
		}
        return false;
    }

    /**
     * Find all DUCCIDs associated with the incoming type.
     *
     * @param type The type enum for the database class, e.g. Job
     * @param isHistory 'true' to search history, 'false' otherwise
     *
     * @return List of human-reaable (aka "friendly" ducc ids as strings.
     */
    public List<Long> listObjectsOfType(DbVertex type, DbCategory dbcat)
    {
        Iterable<Vertex> objs = null;
        if ( dbcat == DbCategory.Any ) {
            objs = graphDb.getVertices(type.pname(), 
                                       new String[] {"@class"},
                                       new Object[]{type.pname()});
        } else {
            graphDb.getVertices(type.pname(), 
                                new String[] {"@class", DbConstants.DUCC_DBCAT}, 
                                new Object[]{type.pname(), dbcat.pname()});
        }
        List<Long> ret = new ArrayList<Long>();

        for ( Vertex v : objs ) {
            OrientVertex ov = (OrientVertex) v;
            ret.add((Long)ov.getProperty(DbConstants.DUCCID));
        }

        return ret;
    }

    /**
     * Convert the vertex property set to a DuccPropeties, scrubbing the
     * ducc metadata.
     */
    Properties vertexToProps(OrientVertex v)
    {
        Properties ret = new DuccProperties();
        Set<String> keys = v.getPropertyKeys();
        for (String k : keys) {
            if ( k.equals(DbConstants.DUCCID) ) continue;
            if ( k.equals(DbConstants.DUCC_DBCAT) ) continue;
            ret.put(k, v.getProperty(k));
        }
        return ret;
    }

    /**
     * Find the objects of the given type and return their Properties, indexed by duccid.
     *
     * NOTE This returns the properties as DuccProperties so you can safely cast and use the
     *      extended function of that class if you want.
     * 
     * @param type The type enum for the object class (e.g. Job)
     * @param isHistory 'true' to search history, 'false' otherwise
     */
    public Map<Long, Properties> getPropertiesForType(DbVertex type, DbCategory dbcat)
    		throws Exception
    {
        String methodName = "getPropertiesForType";
        
        Iterable<Vertex> vs = null;
        if ( dbcat == DbCategory.Any ) {
            vs = graphDb.getVertices(type.pname(), 
                                     new String[] {"@class"}, 
                                     new Object[]{type.pname()});
            
        } else {
            String a = type.pname();
            String[] b = new String[] {"@class", DbConstants.DUCC_DBCAT};
            Object[] c = new Object[]{type.pname(), dbcat.pname()};
            vs = graphDb.getVertices(a, b, c);
                                     
                                     
        }

        Map<Long, Properties> ret = new HashMap<Long, Properties>();

        try {
            for ( Vertex v : vs ) {
                OrientVertex ov = (OrientVertex) v;
                Properties props = vertexToProps(ov);
                Long did = ov.getProperty(DbConstants.DUCCID);
                ret.put(did, props);
            }            
		} catch (Throwable e) {
			logger.error(methodName, null, "Database access error: ", e);
		}

        return ret;
    }

    public Map<Long, Properties> getPropertiesForTypeSel(DbVertex type, DbCategory dbcat)
    		throws Exception
    {
        String methodName = "getPropertiesForType";
        
        Iterable<Vertex> vs = null;
        if ( dbcat == DbCategory.Any ) {
            vs = graphDb.getVertices(type.pname(), 
                                     new String[] {"@class"}, 
                                     new Object[]{type.pname()});
            
        } else {
            vs = select("SELECT FROM " + type.pname() + " WHERE " + DbConstants.DUCC_DBCAT + "='" + dbcat.pname()+"'");
        }

        Map<Long, Properties> ret = new HashMap<Long, Properties>();

        try {
            for ( Vertex v : vs ) {
                OrientVertex ov = (OrientVertex) v;
                Properties props = vertexToProps(ov);
                Long did = ov.getProperty(DbConstants.DUCCID);
                ret.put(did, props);
            }            
		} catch (Throwable e) {
			logger.error(methodName, null, "Database access error: ", e);
		}

        return ret;
    }

    /**
     * Use this for selecting, it returns a set of stuff
     */
    public Iterable<Vertex> select(String sql)
        throws Exception
    {
    	String methodName = "select";
        long now = System.currentTimeMillis();
        logger.info(methodName, null, "SQL", sql);
        logger.info(methodName, null, "Time to select", System.currentTimeMillis() - now);

        return graphDb.command(new OCommandSQL(sql)).execute();
    }

    /**
     * Use this for just executing stuff that returns an int rc
     */
    public int execute(String sql)
    {
    	String methodName = "execute";
        long now = System.currentTimeMillis();
        logger.info(methodName, null, "SQL", sql);
        logger.info(methodName, null, "Time to execute", System.currentTimeMillis() - now);

        return graphDb.command(new OCommandSQL(sql)).execute();
    }

    public void commit()
    {
        String methodName = "commit";
        long now = System.currentTimeMillis();
        if ( graphDb != null ) graphDb.commit();
        logger.info(methodName, null, "Time to commit", System.currentTimeMillis() - now);
    }

    public void rollback()
    {
        String methodName = "rollback";
        logger.warn(methodName, null, "ROLLBACK");
        if ( graphDb != null ) graphDb.
        rollback();
    }

    /**
     * Delete the object of the indicated type and duccid.   We optionally commit in case we want to
     * do more things that have to work under the same transaction so we can rollback if needed.=
     *
     * Nobody is deleting; everything is moved to history.  Later may be utilities to do some cleanup
     * and we'll bring it back.
     */
    // public void deleteObject(DbVertex type, Long duccid, boolean commit)
    //     throws Exception
    // {
    //     String methodName = "deleteObject";
    //     // there usually should only be ONE of these but the API is defined in terms of many
    //     // TODO: throw and rollback if more than one object ( I think, let's revisit this )
    //     Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCCID}, new Object[]{type.pname(), duccid});
    //     for ( Vertex v : s ) {
    //         //logger.info(methodName, null, "Delete vertex for class", type, "id", duccid, "commit", commit);
    //         graphDb.removeVertex(v);
    //     }

    //     if ( commit ) graphDb.commit();
    // }

    /**
     * Plop the object into the DB under the class indicated by 'type', with the
     * unique key 'duccid'.
     *
     * We use id + isHistory + vertex class = primary key and hence must insure they're always set.
     *
     * @prarm type The type enum of the thing to save (e.g. Job)
     * @param duccid The numeric ducc id of the object
     * @param obj The json-ified object to save
     * @param isHistory 'true' if we save to history, 'false' otherwise
     */
    public OrientVertex saveObject(DbVertex type, Long duccid, String obj, DbCategory dbcat)
    {
    	//String methodName = "saveObject";

        //String typename = type.pname();

        OrientVertex ret = null;
        ODocument document = null;
        document = new ODocument(type.pname());
        ret = new OrientVertex(graphDb, document);

        document.fromJSON(obj);
        document.field(DbConstants.DUCCID, duccid);
        document.field(DbConstants.DUCC_DBCAT, dbcat.pname());
        graphDb.getRawGraph().save(document);

        return ret;
    }


    /**
     * Helper class for retrieving an object and all the stuff it points to.  e.g. if you want to
     * reconstitue a DuccWorkJob you need to chase the edges to get the process map and the jd and
     * probably other stuff. 
     *
     * We don't care about history here because the call will have done the right search first.
     *
     * @param v The vertex discovered by the caller.
     * @param read_all 'true' to do recursive traversal down the edges, false otherwise.
     *
     * NOTE: I think the db may have a primitive to do this traversal, 
     *        @TODO must research and use it as it will likely be safer and more efficient.
     * 
     */
    private DbObject traverseObject(OrientVertex v, boolean read_all)
    {
        //String methodName = "traverseObject";
        ODocument doc = v.getRecord();
        String doc_json = doc.toJSON();
        String stype = v.getProperty("@class");
        DbVertex type = manager.vertexType(stype);

        DbObject ret = new DbObject(doc_json, type);

        if ( read_all ) {
            Iterable<Edge> ed = v.getEdges(Direction.OUT);
            for ( Edge e : ed ) {
                OrientEdge oe = (OrientEdge) e;
                OrientVertex ov = oe.getVertex(Direction.IN);
                //logger.info(methodName, null, "Found edge connected to vertex of type", ov.getProperty("@class"), "duccid", ov.getProperty(DUCCID));
                ret.addEmbedded(traverseObject(ov, read_all));
            }
        }
        return ret;
    }

    /**
     * Read a database object, optionally chasing edges to get all the various bits.
     *
     * @param type The type enum of the object, e.g. "Job"
     * @param duccid The numeric id of the object
     * @param read_all 'true' to recursively chase down the edges, 'false' otherwise.
     */
    public DbObject readObject(DbVertex type, Long duccid, boolean read_all)
    {
    	String methodName = "readObject";
        Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCCID}, new Object[]{type.pname(), duccid});
        int count = 0;
        OrientVertex fv = null;
        for (Vertex v : s) {
            fv = (OrientVertex) v;
            //logger.info(methodName, null, "from vertex", count, fv.getIdentity());
            count++;
        }
        if ( count > 1 ) {
            logger.error(methodName, null, "Expected unique object, found", count, "objects. Returning only the first object.");
        }

        //logger.info(methodName, null, "Traversing", fv.getProperty("@class"), "duccid", fv.getProperty(DUCCID));
        return traverseObject(fv, read_all);
    }

    /**
     * Link the 'from' to all the 'to''s' with edges of the right type. Everything is expected
     * to be in the db already, but  maybe not yet committed, according to the caller.
     *
     * @param from the source object 
     * @param to a list of the things the source is pointing/linking to
     * @param type The type enum for edge, e.g. 'process'
     */
    public void addEdges(OrientVertex from, List<OrientVertex> to, DbEdge type)
    {
        //String methodName = "addEdges";
        OrientVertex fv = (OrientVertex) from;
        String etype = type.pname();

        for ( Object o : to ) {
            OrientVertex tv = (OrientVertex) o;
            fv.addEdge(etype, tv);
        }

    }

    
    /**
     * Link the 'from' to a single 'to' with an e right type. Everything is expected
     * to be in the db already, but  maybe not yet committed, according to the caller.
     *
     * @param from the source object 
     * @param a single object to point to
     * @param type The type enum for edge, e.g. 'process'
     */
    public void addEdge(OrientVertex from, OrientVertex to, DbEdge type)
    {
        //String methodName = "addEdges";
        OrientVertex fv = (OrientVertex) from;
        OrientVertex tv = (OrientVertex) to;
        String etype = type.pname();

        fv.addEdge(etype, tv);
    }

    // public void addEdges(DbVertex typeFrom, Long fid, DbVertex typeTo, List<Long> tids)
    // {
    //     String methodName = "addEdges";

    //     Iterable<Vertex> s = graphDb.getVertices(typeFrom.pname(), new String[] {"@class", DUCCID}, new Object[]{typeFrom.pname(), fid});
    //     int count = 0;
    //     OrientVertex fv = null;
    //     for (Vertex v : s) {
    //         fv = (OrientVertex) v;
    //         //logger.info(methodName, null, "from vertex", count, fv.getIdentity());
    //         count++;
    //     }

    //     for ( Long tid : tids ) {
    //         Iterable<Vertex> ss = graphDb.getVertices(typeTo.pname(), new String[] {"@class", DUCCID}, new Object[]{typeTo.pname(), tid});
    //         count = 0;
    //         for (Vertex v : ss) {
    //             OrientVertex tv = (OrientVertex) v;
    //             // logger.info(methodName, null, "to vertex", count, tv.getIdentity());
    //             count++;
                
    //             fv.addEdge(DbEdge.Edge.pname(), tv);
    //         }
    //     }


    // }

    // public void addEdge(DbVertex typeFrom, Long fid, DbVertex typeTo, Long tid)
    // {
    //     String methodName = "addEdge";

    //     Iterable<Vertex> s = graphDb.getVertices(typeFrom.pname(), new String[] {"@class", DUCCID}, new Object[]{typeFrom.pname(), fid});
    //     int count = 0;
    //     OrientVertex fv = null;
    //     for (Vertex v : s) {
    //         fv = (OrientVertex) v;
    //         //logger.info(methodName, null, "from vertex", count, fv.getIdentity());
    //         count++;
    //     }

    //     Iterable<Vertex> ss = graphDb.getVertices(typeTo.pname(), new String[] {"@class", DUCCID}, new Object[]{typeTo.pname(), tid});
    //     count = 0;
    //     for (Vertex v : ss) {
    //         OrientVertex tv = (OrientVertex) v;
    //         // logger.info(methodName, null, "to vertex", count, tv.getIdentity());
    //         count++;

    //         fv.addEdge(DbEdge.Edge.pname(), tv);
    //     }


    // }

    public void changeCategory(OrientVertex obj, DbCategory to)
    {
    	String methodName = "changeCategory";
        Long id = obj.getProperty(DbConstants.DUCCID);
        String oldcat = obj.getProperty(DbConstants.DUCC_DBCAT);
        String type = obj.getProperty("@CLASS");
        logger.info(methodName, null, "Chnage category obj of type", type, "id", id, "from", oldcat, "to", to.pname());
        obj.setProperty(DbConstants.DUCC_DBCAT, to.pname());
    }

    /**
     * Update a single property.  The object must be unique and must already exist.
     *
     * @param keyid DUCCID or DUCC_DBNODE usually. It's the name of the field with the primary key.
     * @param key Holds the value of the primary key
     * @param type The vertex type
     * @param dbcat The category, e.g. history, or checkpoint, or rmstate
     * @param props The properties object to be placed in the db.
     */
    public OrientVertex updateProperty(String keyid, Object key, DbVertex type,  DbCategory dbcat, String propkey, Object propval)
    {
    	String methodName = "updateProperty";
        long now = System.currentTimeMillis();

        Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCC_DBCAT, keyid}, new Object[]{type.pname(), dbcat.pname(), key});
        logger.info(methodName, null, "Time to search on " + type.pname() + " where category " + dbcat.pname() + " and " + keyid + " propkey "  + key, System.currentTimeMillis() - now);
        
        OrientVertex ov = null;
        int count = 0;                        // some sanity checking, we're not allowed more than one
        for (Vertex v : s) {
            ov = (OrientVertex) v;
            count++;
        }
        
        if ( count > 1 ) {
            throw new IllegalStateException("Duplocate object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + key + " propkey "  + propkey + " propval " + propval); 
        }

        if ( ov == null ) {
            throw new IllegalStateException("No object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + key + " propkey "  + propkey + " propval " + propval); 
        }


        ov.setProperty(propkey, propval);
        ov.save();            
        logger.info(methodName, null, "Time to update one property", System.currentTimeMillis() - now);

        return ov;
    }


    public OrientVertex updateProperty(Object dbid, String propkey, Object propval)
    {
    	String methodName = "updateProperty";
        long now = System.currentTimeMillis();
                
        OrientVertex ov = graphDb.getVertex(dbid);
        logger.info(methodName, null, "Time to search on " + dbid.toString(), System.currentTimeMillis() - now);

        if ( ov == null ) {
            throw new IllegalStateException("No object in db: Id " + dbid.toString()); 
        }

        ov.setProperty(propkey, propval);
        ov.save();            
        logger.info(methodName, null, "Time to update one property for", dbid.toString(), System.currentTimeMillis() - now);

        return ov;
    }

    /**
     * Update the properties on an object in the db with the incoming properties.  The object must exist.
     * There is no attempt to synchronize values - the new props either replace, or enrich the object.
     *
     * @param keyid DUCCID or DUCC_DBNODE usually. It's the name of the field with the primary key.
     * @param key Holds the value of the primary key
     * @param type The vertex type
     * @param dbcat The category, e.g. history, or checkpoint, or rmstate
     * @param props A list of (k,v) pairs which will replace their counterparts in the object.
     */
    public OrientVertex updateProperties(String keyid, Object key, DbVertex type,  DbCategory dbcat, Object... props)
    {
    	String methodName = "updateProperties";
        long now = System.currentTimeMillis();
        Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCC_DBCAT, keyid}, new Object[]{type.pname(), dbcat.pname(), key});
        logger.info(methodName, null, "Time to search on " + type.pname() + " where category " + dbcat.pname() + " and " + keyid + " propkey "  + key, System.currentTimeMillis() - now);

        OrientVertex ov = null;
        int count = 0;                        // some sanity checking, we're not allowed more than one
        for (Vertex v : s) {
            ov = (OrientVertex) v;
            count++;
        }

        if ( count > 1 ) {
            throw new IllegalStateException("updateProperties: Duplocate object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + keyid + " propkey "  + key);
        }

        if ( ov == null ) {
            throw new IllegalStateException("updateProperties: No object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + keyid + " propkey "  + key);
        }
        logger.info(methodName, null, "Time to find a vertex", System.currentTimeMillis() - now);

        long now2 = System.currentTimeMillis();     
        ov.setProperties(props);
        ov.save();            
        logger.info(methodName, null, "Time to modify a vertex", System.currentTimeMillis() - now2);

        logger.info(methodName, null, "Time to update several properties", System.currentTimeMillis() - now);

        return ov;
    }

    public OrientVertex updateProperties(Object dbid, Object... props)
    {
    	String methodName = "updateProperties";
        long now = System.currentTimeMillis();
        

        OrientVertex ov = graphDb.getVertex(dbid);

        if ( ov == null ) {
            throw new IllegalStateException("updateProperties: No object in db: id " + dbid.toString());
        }
        logger.info(methodName, null, "Time to find a vertex", dbid.toString(), System.currentTimeMillis() - now);

        long now2 = System.currentTimeMillis();     
        ov.setProperties(props);
        ov.save();            
        logger.info(methodName, null, "Time to modify vertex", dbid.toString(), System.currentTimeMillis() - now2);

        logger.info(methodName, null, "Total time to update vertex", dbid.toString(), System.currentTimeMillis() - now);

        return ov;
    }

    // /**
    //  * Synchronize the properties in an object with the incoming properties.  THe object must be unique
    //  * and already exist.
    //  *
    //  * @param keyid DUCCID or DUCC_DBNODE usually. It's the name of the field with the primary key.
    //  * @param key Holds the value of the primary key
    //  * @param type The vertex type
    //  * @param dbcat The category, e.g. history, or checkpoint, or rmstate
    //  * @param props The properties object to be placed in the db.
    //  */
    // public OrientVertex synchronizeProperties(String keyid, Object key, DbVertex type,  DbCategory dbcat, Properties props)
    // {
    //     String methodName = "synchronizeProperties";
    //     long now = System.currentTimeMillis();

    //     Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCC_DBCAT, keyid}, new Object[]{type.pname(), dbcat.pname(), key});
    //     logger.info(methodName, null, "Time to search " + type.pname() + " category " + dbcat.pname() + " key " + key, System.currentTimeMillis() - now);


    //     long now2 = System.currentTimeMillis();
 
    //     OrientVertex ov = null;
    //     int count = 0;                        // some sanity checking, we're not allowed more than one
    //     for (Vertex v : s) {
    //         ov = (OrientVertex) v;
    //         count++;
    //     }

    //     if ( count > 1 ) {
    //         throw new IllegalStateException("updateProperties: Duplocate object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + key + " propkey "  + key);
    //     }

    //     if ( ov == null ) {
    //         throw new IllegalStateException("updateProperties: No object in db: Type " + type.pname() + " category " + dbcat.pname() + " key " + key + " propkey "  + key);
    //     }
    //     logger.info(methodName, null, "Time to itarate " + type.pname() + " category " + dbcat.pname() + " key " + key, System.currentTimeMillis() - now2);
    //     now2 = System.currentTimeMillis();
                                                  
    //     logger.info(methodName, null, "Update db record of type", type.pname(), "category", dbcat.pname(), "key", key);
    //     Set<String> keys = ov.getPropertyKeys();
    //     for (String k : keys) {
    //         if ( k.equals(DbConstants.DUCCID) )      continue;      // must bypass schema things
    //         if ( k.equals(DbConstants.DUCC_DBCAT) )  continue;
    //         if ( k.equals(DbConstants.DUCC_DBNODE) ) continue;
     
    //         Object val1 = ov.getProperty(k);
    //         Object val2 = props.get(k);
     
    //         if ( val2 == null ) {                                  // the property is removed
    //             logger.info(methodName, null, "Removed property", k);
    //             ov.removeProperty(k);
    //             continue;
    //         }
     
    //         if ( !val2.equals(val1) ) {                            // replace/add a property. val2 is known not null
    //             logger.info(methodName, null, "Replaced/added property", k, "value", val1, "with", val2);
    //             ov.setProperty(k, val2);
    //             continue;
    //         }
    //     }
 
    //     ov.setProperty(keyid, key);
    //     ov.save();            
    //     logger.info(methodName, null, "Time to update " + type.pname() + " category " + dbcat.pname() + " key " + key, System.currentTimeMillis() - now2);

    //     logger.info(methodName, null, "Time to synchronize properties", System.currentTimeMillis() - now);

    //     return ov;
    // }

    // public OrientVertex synchronizeProperties(Object dbid, Properties props)
    // {
    //     String methodName = "synchronizeProperties";
    //     long now = System.currentTimeMillis();



    //     long now2 = System.currentTimeMillis();
 
    //     OrientVertex ov = graphDb.getVertex(dbid);
    //     if ( ov == null ) {
    //         throw new IllegalStateException("updateProperties: No object in db. Id " + dbid);
    //     }
    //     logger.info(methodName, null, "Time to search for ", dbid, System.currentTimeMillis() - now);

    //     now2 = System.currentTimeMillis();                                                  
    //     logger.info(methodName, null, "Update db record ", dbid);
    //     Set<String> keys = ov.getPropertyKeys();
    //     for (String k : keys) {
    //         if ( k.equals(DbConstants.DUCCID) )      continue;      // must bypass schema things
    //         if ( k.equals(DbConstants.DUCC_DBCAT) )  continue;
    //         if ( k.equals(DbConstants.DUCC_DBNODE) ) continue;

    //         if ( k.equals("svc_dbid") ) continue;
    //         if ( k.equals("meta_dbid") ) continue;

    //         Object val1 = ov.getProperty(k);
    //         Object val2 = props.get(k);
     
    //         if ( val2 == null ) {                                  // the property is removed
    //             logger.info(methodName, null, "Removed property", k);
    //             ov.removeProperty(k);
    //             continue;
    //         }
     
    //         if ( !val2.equals(val1) ) {                            // replace/add a property. val2 is known not null
    //             logger.info(methodName, null, "Replaced/added property", k, "value", val1, "with", val2);
    //             ov.setProperty(k, val2);
    //             continue;
    //         }
    //     }
 
    //     ov.save();            
    //     logger.info(methodName, null, "Time to update record", dbid, System.currentTimeMillis() - now2);
    //     logger.info(methodName, null, "Total time to synchronize record", dbid, System.currentTimeMillis() - now);

    //     return ov;
    //    }

    /**
     * Create an object in the db from a properties object.  The caller must do the the checking to insure
     * the object already exists (or not, e.g. for a db loader).
     *
     * @param props The properties object to be placed in the db.
     * @param type The type enum for the object, e.g. "Service"
     * @param duccid The numeric id of the object
     * @param isHistory 'True' if it is to be placed in history, 'false' otherwise.
     */
    public OrientVertex createProperties(String keyid, Object key, DbVertex type, DbCategory dbcat, Properties props)
    {
    	String methodName = "createPropertiesObject";
        String typeName = type.pname();
        OrientVertex ov = null;

        logger.info(methodName, null, "Create new properties object of type", type.pname(), "category", dbcat.pname(), "key", key); 
        ov = graphDb.addVertex("class:" + typeName, keyid, key, DbConstants.DUCC_DBCAT, dbcat.pname());
        ov.setProperties(props);
        return ov;
    }

    static Gson gson = null;
    static Gson mkGsonForJob()
    {
        synchronized(DbHandle.class) {
            if ( gson != null ) return gson;

            // We need to define Instance creators and such so we do it in a common place
            GsonBuilder gb = new GsonBuilder();
            
            GenericInterfaceAdapter customAdapter = new GenericInterfaceAdapter();
            gb.serializeSpecialFloatingPointValues().setPrettyPrinting();
            gb.enableComplexMapKeySerialization();
            
            gb.registerTypeAdapter(Node.class, new NodeInstanceCreator());
            gb.registerTypeAdapter(NodeIdentity.class, new NodeIdentityCreator());
            
            //gb.registerTypeAdapter(IIdentity.class, new IdentityInstanceCreator());
            gb.registerTypeAdapter(IIdentity.class, customAdapter);
            
            gb.registerTypeAdapter(IDuccId.class, customAdapter);
            gb.registerTypeAdapter(ICommandLine.class, customAdapter);
            gb.registerTypeAdapter(ITimeWindow.class, customAdapter);
            gb.registerTypeAdapter(IDuccProcessWorkItems.class, customAdapter);
            gb.registerTypeAdapter(IDuccUimaAggregateComponent.class, customAdapter);
            gb.registerTypeAdapter(IUimaPipelineAEComponent.class, customAdapter);
            gb.registerTypeAdapter(IRationale.class, customAdapter);
            gb.registerTypeAdapter(IDuccUimaDeployableConfiguration.class, customAdapter);
            gb.registerTypeAdapter(IDuccStandardInfo.class, customAdapter);
            gb.registerTypeAdapter(IDuccSchedulingInfo.class, customAdapter);
            gb.registerTypeAdapter(IDuccPerWorkItemStatistics.class, customAdapter);
            gb.registerTypeAdapter(IDuccReservationMap.class, customAdapter);
            gb.registerTypeAdapter(JdReservationBean.class, customAdapter);
            
            //ConcurrentHashMap<DuccId, Long> x = new ConcurrentHashMap<DuccId, Long>();
            //gb.registerTypeAdapter(x.getClass(), new MapAdaptor());
            
            //gb.registerTypeAdapterFactory(new DuccTypeFactory());
            //Object obj = new ArrayList<IJdReservation>();
            //gb.registerTypeAdapter(obj.getClass(), customAdapter);
            Gson g = gb.create();
            return g;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Instance creators and adaptors for GSON
    // ----------------------------------------------------------------------------------------------------

    // We need these for the DuccNode and NodeIdentity because they don't have no-arg
    // Constructors.  
    //
    // @TODO after merge, consult with Jerry about adding in those constructors
    private static class NodeInstanceCreator implements InstanceCreator<Node> {
        public Node createInstance(Type type) {
            //            System.out.println("DuccNode");
            return new DuccNode(null, null, false);
        }
    }

    private static class NodeIdentityCreator implements InstanceCreator<NodeIdentity> {
        public NodeIdentity createInstance(Type type) {
            //            System.out.println("DuccNodeIdentity");
            try { return new NodeIdentity(null, null); } catch ( Exception e ) {}
            return null;
        }
    }

    /**
     * JSON helper for our complex objects.  Gson doesn't save type information in the json so
     * it doesn't know how to construct things declared as interfaces.
     *
     * This class is a Gson adapter that saves the actual object type in the json on serialization,
     * and uses that information on deserialization to construct the right thing.
     */
    private static class GenericInterfaceAdapter
        implements
            JsonSerializer<Object>, 
            JsonDeserializer<Object> 
    {

        private static final String DUCC_META_CLASS = "DUCC_META_CLASS";
        
        @Override
        public Object deserialize(JsonElement jsonElement, 
                                  Type type,
                                  JsonDeserializationContext jsonDeserializationContext)
        throws JsonParseException 
        {
            // Reconstitute the "right" class based on the actual class it came from as
            // found in metadata
            JsonObject  obj    = jsonElement.getAsJsonObject();
            JsonElement clElem= obj.get(DUCC_META_CLASS);

            if ( clElem== null ) {
                throw new IllegalStateException("Cannot determine concrete class for " + type + ". Must register explicit type adapter for it.");
            }
            String clName = clElem.getAsString();

            //System.out.println("----- elem: " + clName + " clElem: " + obj);
            try {
                Class<?> clz = Class.forName(clName);
                return jsonDeserializationContext.deserialize(jsonElement, clz);
            } catch (ClassNotFoundException e) {
                throw new JsonParseException(e);
            }
        }
        
        @Override
        public JsonElement serialize(Object object, 
                                     Type type,
                                     JsonSerializationContext jsonSerializationContext) 
        {
            // Add the mete element indicating what kind of concrete class is this came from
            //String n = object.getClass().getCanonicalName();
            //System.out.println("**** Serialize object A " + n + " of type " + type);
            //if ( n.contains("Concurrent") ) {
             //   int stop = 1;
               // stop++;
            //}

            JsonElement ele = jsonSerializationContext.serialize(object, object.getClass());
            //System.out.println("**** Serialize object B " + object.getClass().getCanonicalName() + " of type " + type + " : ele " + ele);
            ele.getAsJsonObject().addProperty(DUCC_META_CLASS, object.getClass().getCanonicalName());
            return ele;
        }
    }

    @SuppressWarnings("unused")
	private class DuccTypeFactory 
        implements TypeAdapterFactory
    {

        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) 
        {
            //System.out.println("TYPETOKEN: " + typeToken + " raw type: " + typeToken.getRawType().getName());
            Class<?> cl = typeToken.getRawType();
            //System.out.println("         Canonical name: " + cl.getCanonicalName());
            Type type = typeToken.getType();
            if ( typeToken.getRawType() != ConcurrentHashMap.class ) {
                //System.out.println("Skipping type " + typeToken);
                return null;
            }

            if ( type instanceof ParameterizedType ) {
                
                ParameterizedType pt = (ParameterizedType) type;
                Type[] types = pt.getActualTypeArguments();
                //for ( Type tt : types ) {
                    // System.out.println("     TYPE ARGUMENTS: " + tt);
                //}
                Type tt = types[0];
                Class<?> cll = (Class<?>) tt;
                
            }
            return null;
        }

    }

    @SuppressWarnings("unused")
	private class MapAdaptor
        extends TypeAdapter<ConcurrentHashMap<DuccId, Long>>
    {
            
        public void write(JsonWriter out, ConcurrentHashMap<DuccId, Long> map) throws IOException {
            System.out.println("***************** Writing");
            if (map == null) {
                out.nullValue();
                return;
            }

            out.beginArray();
            for (DuccId k : map.keySet() ) {
                out.beginObject();
                out.value(k.getFriendly());
                out.value(k.getUnique());
                out.value(map.get(k));
                out.endObject();
            }
            out.endArray();
        }
                
        public ConcurrentHashMap<DuccId, Long> read(JsonReader in) throws IOException {
            System.out.println("***************** reading");
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
                    
            ConcurrentHashMap<DuccId, Long> ret = new ConcurrentHashMap<DuccId, Long>();
            in.beginArray();
            while (in.hasNext()) {
                in.beginObject();
                Long friendly = in.nextLong();
                String unique = in.nextString();

                Long val = in.nextLong();
                in.endObject();
                DuccId id = new DuccId(friendly);
                id.setUUID(UUID.fromString(unique));
                ret.put(id, val);
            }
            in.endArray();
            return ret;
        }
    }
}
