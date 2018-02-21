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

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.persistence.or.IDbDuccWorks;
import org.apache.uima.ducc.common.persistence.or.ITypedProperties;
import org.apache.uima.ducc.common.persistence.or.TypedProperties;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.gson.Gson;

public class DbDuccWorks implements IDbDuccWorks {
	
	/*
	 * table comprising specifications for Jobs, Managed Reservations
	 */
	private static String DUCC_WORKS_TABLE = DbDuccWorks.TABLE_NAME.pname();

	private static String COL_TYPE = DbDuccWorks.type.columnName();
	private static String COL_DUCC_ID = DbDuccWorks.ducc_id.columnName();
	private static String COL_SPECIFICATION = DbDuccWorks.specification.columnName();
	
	private DuccLogger logger = null;
	private DbManager dbManager = null;

	private Gson gson = new Gson();
	
	private DuccId jobid = null;
	
	public DbDuccWorks(DuccLogger duccLogger) throws Exception {
		init(duccLogger);
	}
	
	private String messageDbDisabled = "db disabled";
	private String dbHost = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_database_host, "?");
    
	private boolean isDbDisabled() {
    	boolean retVal = false;
    	if(dbHost.equals(DuccPropertiesResolver.ducc_database_disabled)) {
    		retVal = true;
    	}
    	return retVal;
    }
	
	/*
	 * connect to DB
	 */
	private boolean init(String dburl) throws Exception {
		String methodName = "init";
		boolean ret = false;
		while (true) {
			try {
				dbManager = new DbManager(dburl, logger);
				dbManager.init();
				ret = true;
				break;
			} catch (NoHostAvailableException e) {
				logger.error(methodName, null,
						"Cannot contact database.  Retrying in 5 seconds.");
				Thread.sleep(5000);
			} catch (Exception e) {
				logger.error(methodName, null,
						"Errors contacting database.  No connetion made.");
				logger.error(methodName, null, e);
				ret = false;
				break;
			}
		}
		return ret;
	}

	/*
	 * CQL to create:
	 *   Specifications table, keyed by type (Job, ManagedReservation) + DuccId
	 */
	protected static List<SimpleStatement> mkSchema() throws Exception {
		List<SimpleStatement> ret = new ArrayList<SimpleStatement>();

		StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS "
				+ DUCC_WORKS_TABLE + " (");
		buf.append(DbUtil.mkSchema(DbDuccWorks.values()));
		buf.append(")");
		ret.add(new SimpleStatement(buf.toString()));
		List<String> indexes = DbUtil.mkIndices(DbDuccWorks.values(),
				DUCC_WORKS_TABLE);
		for (String s : indexes) {
			ret.add(new SimpleStatement(s));
		}

		return ret;
	}

	/**
	 * Create tables(s)
	 */
	public void dbInit() throws Exception {
		String location = "dbInit";
		if(isDbDisabled()) {
			logger.debug(location, jobid, messageDbDisabled);
			return;
		}
		try {
			List<SimpleStatement>specificationsSchema = mkSchema();
			DbHandle h = dbManager.open();
	        for ( SimpleStatement s : specificationsSchema ) {
	            logger.info(location, jobid, "EXECUTE STATEMENT:"+s.toString());
	            h.execute(s);
	        }
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			throw e;
		}
	}
	
	/**
	 * Prepare for DB access
	 */
	@Override
	public void init(DuccLogger duccLogger) throws Exception {
		 String location = "init";
		 this.logger = duccLogger;
		 if(isDbDisabled()) {
			 logger.debug(location, jobid, messageDbDisabled);
			 return;
		 }
	     String dbUrl = System.getProperty(DbManager.URL_PROPERTY);
	     init(dbUrl);
	}

	/**
	 * Add or update a specification of type Job or ManagedReservation
	 * 
	 * SPECIFICATION data is kept in DB as JSON comprising:
	 * { map: system: { k1:v1, k2:v2,... }, user: { k1:v1, k2:v2,... } } 
	 */
	@Override
	public void upsertSpecification(String type, long id, ITypedProperties properties) throws Exception {
		String location = "upsertSpecification";
		String gsonString = null;
		if(isDbDisabled()) {
			logger.debug(location, jobid, messageDbDisabled);
			return;
		}
		try {
			gsonString = gson.toJson(properties);
			String table = DUCC_WORKS_TABLE;
			String c0 = COL_SPECIFICATION+"="+"'"+gsonString+"'";
			String c1 = COL_TYPE+"="+"'"+type+"'";
			String c2 = COL_DUCC_ID+"="+id;
	        String cql = "UPDATE "+table+" SET "+c0+" WHERE "+c1+" AND "+c2;
	        logger.debug(location, jobid, cql);
	        DbHandle h = dbManager.open();
	        h.execute(cql);
		}
		catch(Exception e) {
			DuccId duccid = new DuccId(id);
			String text = "type="+type+" "+"gson="+gsonString;
			logger.error(location, duccid, text ,e);
			throw e;
		}
	}
	
	/**
	 * Retrieve a specification of type Job or ManagedReservation
	 */
	@Override
	public ITypedProperties fetchSpecification(String type, long id) throws Exception {
		String location = "fetchSpecification";
		ITypedProperties properties = null;
		if(isDbDisabled()) {
			logger.debug(location, jobid, messageDbDisabled);
			return properties;
		}
		try {
			String table = DUCC_WORKS_TABLE;
			String c1 = COL_TYPE+"="+"'"+type+"'";
			String c2 = COL_DUCC_ID+"="+id;
	        String cql = "SELECT * FROM "+table+" WHERE "+c1+" AND "+c2;
	        logger.debug(location, jobid, cql);
	        DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        for ( Row r : rs ) {
	            String gsonString = r.getString(COL_SPECIFICATION);
	            logger.debug(location, jobid, gsonString);
	            properties = (ITypedProperties) gson.fromJson(gsonString, TypedProperties.class);
	        }
		}
		catch(Exception e) {
			DuccId duccid = new DuccId(id);
			String text = "type="+type;
			logger.error(location, duccid, text ,e);
			throw e;
		}
        return properties;
	}

}
