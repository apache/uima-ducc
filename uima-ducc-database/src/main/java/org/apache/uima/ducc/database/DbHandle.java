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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;

public class DbHandle
{
    private DuccLogger logger = null;
    
    DbManager manager;

    DbHandle(DbManager manager)
    {
        logger = DuccLogger.getLogger(DbHandle.class, "DB");
//         if ( DuccService.getDuccLogger() == null ) {
//             // not running within a ducc service - just get a regular logger
//             logger = DuccLogger.getLogger(DbHandle.class, "DB");
//         } else {
//             // running within a ducc service - get the component logger
//             logger = DuccService.getDuccLogger(DbHandle.class.getName());
//         }
        this.manager = manager;
    }
        
    public ResultSet execute(String sql)
        throws Exception
    {
        String methodName = "execute";
        long now = System.currentTimeMillis();
        ResultSet ret = manager.execute(sql);
        if ( logger.isDebug() ) logger.debug(methodName, null, "Time to execute", System.currentTimeMillis() - now);
        
        return ret;
    }

    public ResultSet execute(SimpleStatement s)
    {
        return manager.execute(s);
    }

    public ResultSet execute(BoundStatement s)
    {
        return manager.execute(s);
    }

    public ResultSet execute(PreparedStatement ps, Object ... fields)
        throws Exception
    {
        String methodName = "execute";        
        long now = System.currentTimeMillis();
        
        try {
			BoundStatement boundStatement = new BoundStatement(ps);
			BoundStatement bound = boundStatement.bind(fields);
			return execute(bound);        
        } finally {
			if ( logger.isTrace() ) {
                logger.trace(methodName, null, "Time to execute prepared statement:", ps.getQueryString(), System.currentTimeMillis() - now);
                StringBuffer buf = new StringBuffer("Fields for statement: ");
                for ( Object o: fields ) {
                    buf.append(o.toString());
                    buf.append(" ");
                }
                logger.trace(methodName, null, buf.toString());
            }
		}
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
//     public OrientVertex saveObject(DbVertex type, Long duccid, String obj, DbCategory dbcat)
//     {
//     	//String methodName = "saveObject";

//         //String typename = type.pname();

//         OrientVertex ret = null;
//         ODocument document = null;
//         document = new ODocument(type.pname());
//         ret = new OrientVertex(graphDb, document);

//         document.fromJSON(obj);
//         document.field(DbConstants.DUCCID, duccid);
//         document.field(DbConstants.DUCC_DBCAT, dbcat.pname());
//         graphDb.getRawGraph().save(document);

//         return ret;
//     }


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
//     private DbObject traverseObject(OrientVertex v, boolean read_all)
//     {
//         //String methodName = "traverseObject";
//         ODocument doc = v.getRecord();
//         String doc_json = doc.toJSON();
//         String stype = v.getProperty("@class");
//         DbVertex type = manager.vertexType(stype);

//         DbObject ret = new DbObject(doc_json, type);

//         if ( read_all ) {
//             Iterable<Edge> ed = v.getEdges(Direction.OUT);
//             for ( Edge e : ed ) {
//                 OrientEdge oe = (OrientEdge) e;
//                 OrientVertex ov = oe.getVertex(Direction.IN);
//                 //logger.info(methodName, null, "Found edge connected to vertex of type", ov.getProperty("@class"), "duccid", ov.getProperty(DUCCID));
//                 ret.addEmbedded(traverseObject(ov, read_all));
//             }
//         }
//         return ret;
//     }

    /**
     * Read a database object, optionally chasing edges to get all the various bits.
     *
     * @param type The type enum of the object, e.g. "Job"
     * @param duccid The numeric id of the object
     * @param read_all 'true' to recursively chase down the edges, 'false' otherwise.
     */
//     public DbObject readObject(DbVertex type, Long duccid, boolean read_all)
//     {
//     	String methodName = "readObject";
//         Iterable<Vertex> s = graphDb.getVertices(type.pname(), new String[] {"@class", DbConstants.DUCCID}, new Object[]{type.pname(), duccid});
//         int count = 0;
//         OrientVertex fv = null;
//         for (Vertex v : s) {
//             fv = (OrientVertex) v;
//             //logger.info(methodName, null, "from vertex", count, fv.getIdentity());
//             count++;
//         }
//         if ( count > 1 ) {
//             logger.error(methodName, null, "Expected unique object, found", count, "objects. Returning only the first object.");
//         }

//         //logger.info(methodName, null, "Traversing", fv.getProperty("@class"), "duccid", fv.getProperty(DUCCID));
//         return traverseObject(fv, read_all);
//     }

    /**
     * This views a row as a set of properties.  We update a single column in the table.
     *
     * @param table The table name.
     * @param row   The key to search on for update.  Caller must fully-form it e.g. "name=bob", if the key
     *                  field is called 'name'.
     * @param propkey The name of the column.
     * @param propval The value to replace the existing value.
     */
    public boolean updateProperty(String table, String row, String propkey, Object propval)
        throws Exception
    {
    	String methodName = "updateProperty";
        long now = System.currentTimeMillis();
        
        String cql = "UPDATE " + table + " SET " + propkey + "=" + propval + " WHERE " + row;
        try {
            manager.execute(cql);
            return true;
        } finally {
            logger.debug(methodName, null, "Time to update one property", propkey, System.currentTimeMillis() - now);        
        }
    }
    
    /**
     * This views a row as a set of properties.  We update multiple columns in a single row of the table.
     *
     * @param table The table name.
     * @param row   The key to search on for update.  Caller must fully-form it e.g. "name=bob", if the key
     *                  field is called 'name'.
     * @param props This is a list of propertes where every even-numbered object is a column name and
     *                  every odd-numbered object is its value.
     * 
     * @throws IllegalArgumentException if the number of 'props' is not even.  Lower layers may throw
     *                  unchecked exceptions also.
     */
    public void updateProperties(String table, String row, Object... props)
        throws Exception
    {
    	String methodName = "updateProperties";
        long now = System.currentTimeMillis();

        if ( ( props.length % 2 ) != 0 ) {
            throw new IllegalArgumentException("mkUpdate: need even number of props to form (k,v) list.  Found " + props.length + " props.");
        }

        String cql = DbUtil.mkUpdate(table, row, props);
        try {
            logger.trace(methodName, null, cql);
            manager.execute(cql);
        } finally {
            logger.debug(methodName, null, "Total time to update properties", System.currentTimeMillis() - now);
        }
    }


    public PreparedStatement prepare(String cql)
    {
        //String methodName = "prepare";
        return manager.prepare(cql);
    }

    void truncate(String table)
    	throws Exception
    {
        manager.truncate(table);
    }


    void saveObject(PreparedStatement ps, Object ... fields)
        throws Exception
    {
        String methodName = "saveObject";        
        long now = System.currentTimeMillis();

        BoundStatement boundStatement = new BoundStatement(ps);
        BoundStatement bound = boundStatement.bind(fields);
        execute(bound);        
        logger.debug(methodName, null, "Time to execute prepared statement:", ps.getQueryString(), System.currentTimeMillis() - now);
    }

    /**
     * Create an object in the db from a properties object.  The caller must do the the checking to insure
     * the object already exists (or not, e.g. for a db loader).
     *
     * @param props The properties object to be placed in the db.
     * @param type The type enum for the object, e.g. "Service"
     * @param duccid The numeric id of the object
     * @param isHistory 'True' if it is to be placed in history, 'false' otherwise.
     */
//     public OrientVertex createProperties(String keyid, Object key, DbVertex type, DbCategory dbcat, Properties props)
//     {
//     	String methodName = "createPropertiesObject";
//         String typeName = type.pname();
//         OrientVertex ov = null;

//         logger.info(methodName, null, "Create new properties object of type", type.pname(), "category", dbcat.pname(), "key", key); 
//         ov = graphDb.addVertex("class:" + typeName, keyid, key, DbConstants.DUCC_DBCAT, dbcat.pname());
//         ov.setProperties(props);
//         return ov;
//     }

//     static Gson gson = null;
//     static Gson mkGsonForJob()
//     {
//         synchronized(DbHandle.class) {
//             if ( gson != null ) return gson;

//             // We need to define Instance creators and such so we do it in a common place
//             GsonBuilder gb = new GsonBuilder();
            
//             GenericInterfaceAdapter customAdapter = new GenericInterfaceAdapter();
//             gb.serializeSpecialFloatingPointValues().setPrettyPrinting();
//             gb.enableComplexMapKeySerialization();
            
//             gb.registerTypeAdapter(Node.class, new NodeInstanceCreator());
//             gb.registerTypeAdapter(NodeIdentity.class, new NodeIdentityCreator());
            
//             //gb.registerTypeAdapter(IIdentity.class, new IdentityInstanceCreator());
//             gb.registerTypeAdapter(IIdentity.class, customAdapter);
            
//             gb.registerTypeAdapter(IDuccId.class, customAdapter);
//             gb.registerTypeAdapter(ICommandLine.class, customAdapter);
//             gb.registerTypeAdapter(ITimeWindow.class, customAdapter);
//             gb.registerTypeAdapter(IDuccProcessWorkItems.class, customAdapter);
//             gb.registerTypeAdapter(IDuccUimaAggregateComponent.class, customAdapter);
//             gb.registerTypeAdapter(IUimaPipelineAEComponent.class, customAdapter);
//             gb.registerTypeAdapter(IRationale.class, customAdapter);
//             gb.registerTypeAdapter(IDuccUimaDeployableConfiguration.class, customAdapter);
//             gb.registerTypeAdapter(IDuccStandardInfo.class, customAdapter);
//             gb.registerTypeAdapter(IDuccSchedulingInfo.class, customAdapter);
//             gb.registerTypeAdapter(IDuccPerWorkItemStatistics.class, customAdapter);
//             gb.registerTypeAdapter(IDuccReservationMap.class, customAdapter);
//             gb.registerTypeAdapter(JdReservationBean.class, customAdapter);
            
//             //ConcurrentHashMap<DuccId, Long> x = new ConcurrentHashMap<DuccId, Long>();
//             //gb.registerTypeAdapter(x.getClass(), new MapAdaptor());
            
//             //gb.registerTypeAdapterFactory(new DuccTypeFactory());
//             //Object obj = new ArrayList<IJdReservation>();
//             //gb.registerTypeAdapter(obj.getClass(), customAdapter);
//             Gson g = gb.create();
//             return g;
//         }
//     }

//     // ----------------------------------------------------------------------------------------------------
//     // Instance creators and adaptors for GSON
//     // ----------------------------------------------------------------------------------------------------

//     // We need these for the DuccNode and NodeIdentity because they don't have no-arg
//     // Constructors.  
//     //
//     // @TODO after merge, consult with Jerry about adding in those constructors
//     private static class NodeInstanceCreator implements InstanceCreator<Node> {
//         public Node createInstance(Type type) {
//             //            System.out.println("DuccNode");
//             return new DuccNode(null, null, false);
//         }
//     }

//     private static class NodeIdentityCreator implements InstanceCreator<NodeIdentity> {
//         public NodeIdentity createInstance(Type type) {
//             //            System.out.println("DuccNodeIdentity");
//             try { return new NodeIdentity(null, null); } catch ( Exception e ) {}
//             return null;
//         }
//     }

//     /**
//      * JSON helper for our complex objects.  Gson doesn't save type information in the json so
//      * it doesn't know how to construct things declared as interfaces.
//      *
//      * This class is a Gson adapter that saves the actual object type in the json on serialization,
//      * and uses that information on deserialization to construct the right thing.
//      */
//     private static class GenericInterfaceAdapter
//         implements
//             JsonSerializer<Object>, 
//             JsonDeserializer<Object> 
//     {

//         private static final String DUCC_META_CLASS = "DUCC_META_CLASS";
        
//         @Override
//         public Object deserialize(JsonElement jsonElement, 
//                                   Type type,
//                                   JsonDeserializationContext jsonDeserializationContext)
//         throws JsonParseException 
//         {
//             // Reconstitute the "right" class based on the actual class it came from as
//             // found in metadata
//             JsonObject  obj    = jsonElement.getAsJsonObject();
//             JsonElement clElem= obj.get(DUCC_META_CLASS);

//             if ( clElem== null ) {
//                 throw new IllegalStateException("Cannot determine concrete class for " + type + ". Must register explicit type adapter for it.");
//             }
//             String clName = clElem.getAsString();

//             //System.out.println("----- elem: " + clName + " clElem: " + obj);
//             try {
//                 Class<?> clz = Class.forName(clName);
//                 return jsonDeserializationContext.deserialize(jsonElement, clz);
//             } catch (ClassNotFoundException e) {
//                 throw new JsonParseException(e);
//             }
//         }
        
//         @Override
//         public JsonElement serialize(Object object, 
//                                      Type type,
//                                      JsonSerializationContext jsonSerializationContext) 
//         {
//             // Add the mete element indicating what kind of concrete class is this came from
//             //String n = object.getClass().getCanonicalName();
//             //System.out.println("**** Serialize object A " + n + " of type " + type);
//             //if ( n.contains("Concurrent") ) {
//              //   int stop = 1;
//                // stop++;
//             //}

//             JsonElement ele = jsonSerializationContext.serialize(object, object.getClass());
//             //System.out.println("**** Serialize object B " + object.getClass().getCanonicalName() + " of type " + type + " : ele " + ele);
//             ele.getAsJsonObject().addProperty(DUCC_META_CLASS, object.getClass().getCanonicalName());
//             return ele;
//         }
//     }

//     @SuppressWarnings("unused")
// 	private class DuccTypeFactory 
//         implements TypeAdapterFactory
//     {

//         public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) 
//         {
//             //System.out.println("TYPETOKEN: " + typeToken + " raw type: " + typeToken.getRawType().getName());
//             Class<?> cl = typeToken.getRawType();
//             //System.out.println("         Canonical name: " + cl.getCanonicalName());
//             Type type = typeToken.getType();
//             if ( typeToken.getRawType() != ConcurrentHashMap.class ) {
//                 //System.out.println("Skipping type " + typeToken);
//                 return null;
//             }

//             if ( type instanceof ParameterizedType ) {
                
//                 ParameterizedType pt = (ParameterizedType) type;
//                 Type[] types = pt.getActualTypeArguments();
//                 //for ( Type tt : types ) {
//                     // System.out.println("     TYPE ARGUMENTS: " + tt);
//                 //}
//                 Type tt = types[0];
//                 Class<?> cll = (Class<?>) tt;
                
//             }
//             return null;
//         }

//     }

//     @SuppressWarnings("unused")
// 	private class MapAdaptor
//         extends TypeAdapter<ConcurrentHashMap<DuccId, Long>>
//     {
            
//         public void write(JsonWriter out, ConcurrentHashMap<DuccId, Long> map) throws IOException {
//             System.out.println("***************** Writing");
//             if (map == null) {
//                 out.nullValue();
//                 return;
//             }

//             out.beginArray();
//             for (DuccId k : map.keySet() ) {
//                 out.beginObject();
//                 out.value(k.getFriendly());
//                 out.value(k.getUnique());
//                 out.value(map.get(k));
//                 out.endObject();
//             }
//             out.endArray();
//         }
                
//         public ConcurrentHashMap<DuccId, Long> read(JsonReader in) throws IOException {
//             System.out.println("***************** reading");
//             if (in.peek() == JsonToken.NULL) {
//                 in.nextNull();
//                 return null;
//             }
                    
//             ConcurrentHashMap<DuccId, Long> ret = new ConcurrentHashMap<DuccId, Long>();
//             in.beginArray();
//             while (in.hasNext()) {
//                 in.beginObject();
//                 Long friendly = in.nextLong();
//                 String unique = in.nextString();

//                 Long val = in.nextLong();
//                 in.endObject();
//                 DuccId id = new DuccId(friendly);
//                 id.setUUID(UUID.fromString(unique));
//                 ret.put(id, val);
//             }
//             in.endArray();
//             return ret;
//         }
//     }
}

