package org.apache.uima.ducc.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;

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
    private DuccLogger logger = DuccLogger.getLogger(DbHandle.class, "DB");  // get the component logger
    
    DbManager manager;
    public OrientBaseGraph graphDb;

    DbHandle(DbManager manager, OrientBaseGraph graphDb)
    {
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

    /**
     * Use this for selecting, it returns a set of stuff
     */
    public Iterable<Vertex> select(String sql)
        throws Exception
    {
    	String methodName = "select";
        logger.info(methodName, null, "SQL", sql);
        return graphDb.command(new OCommandSQL(sql)).execute();
    }

    /**
     * Use this for just executing stuff that returns an int rc
     */
    public int execute(String sql)
    {
    	String methodName = "execute";
        logger.info(methodName, null, "SQL", sql);
        return graphDb.command(new OCommandSQL(sql)).execute();
    }

    public void commit()
    {
        if ( graphDb != null ) graphDb.commit();
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
    public Object saveObject(DbVertex type, Long duccid, String obj, DbCategory dbcat)
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
    public void addEdges(Object from, List<Object> to, DbEdge type)
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
    public void addEdge(Object from, Object to, DbEdge type)
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

    /**
     * Create an object in the db from a properties object.
     *
     * @param props The properties object to be placed in the db.
     * @param type The type enum for the object, e.g. "Service"
     * @param duccid The numeric id of the object
     * @param isHistory 'True' if it is to be placed in history, 'false' otherwise.
     */
    public Object createPropertiesObject(Properties props, DbVertex type, Long duccid, DbCategory dbcat)
    {
        // Note: caller must insure this is first time for this if he doesn't want a duplicate.
        //       by calling thingInDatabase().

    	//String methodName = "createPropertiesObject";
        String typeName = type.pname();

        OrientVertex ov = null;

        // logger.info(methodName, null, duccid, "Create new db record of type", typeName); 
        ov = graphDb.addVertex("class:" + typeName, DbConstants.DUCCID, duccid, DbConstants.DUCC_DBCAT, dbcat.pname());
        ov.setProperties(props);
        return ov;
    }

    /**
     * Use the incoming properties to set the properties on the object of given type and duccid.
     * Rules:
     *    1. If the object does not exist in the db, add it with no properties.
     *    2. If the property exists in both, update the value.
     *    3. If the property exists only in db object, delete from the db object.
     *    4. If the property exists only in input, add to db object.
     *    5. Caller must commit, allowing for multiple things in a transaction
     * @param props The propertiess to sync with
     * @param type The type of object to update (e.g. Service, ServiceMeta, Job etc)
     * @param duccid the duccid of the object
     * @param isHistory 'True' if the object is to be placed in history, 'false' otherwise
     */
    public void syncProperties(Properties props, DbVertex type, Long duccid, DbCategory dbcat)
        throws Exception
    {
    	//String methodName = "syncProperties";

        // The assumption is that only one object of the given DbVertex.type and duccid is allowed in the
        // database.  
        Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCCID}, new Object[]{type.pname(), duccid});
        
        OrientVertex ov = null;
        int count = 0;                        // some sanity checking, we're not allowed more than one
        for (Vertex v : s) {
            ov = (OrientVertex) v;
            count++;
        }

        if ( count > 1 ) {
            throw new IllegalStateException("Multiple database records for " + type + "." + duccid);
        }        

        if ( count == 0 ) {
            throw new IllegalStateException("No record found to update for " + type + "." + duccid);
        }

        //logger.info(methodName, null, duccid, "Update record of type", type);
        Set<String> keys = ov.getPropertyKeys();
        for (String k : keys) {            // (clear a property according to rule 3 above)
            if ( ! k.equals(DbConstants.DUCCID) && !props.containsKey(k) ) {
                ov.removeProperty(k);
            }
        }
        ov.setProperties(props);     // handles both rules 2 and 4
        ov.setProperty(DbConstants.DUCC_DBCAT, dbcat.pname());
        //graphDb.getRawGraph().save(ov.getRecord());
        ov.save();

    }

}
