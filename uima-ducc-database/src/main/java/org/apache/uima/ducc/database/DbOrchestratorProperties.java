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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class DbOrchestratorProperties implements IDbOrchestratorProperties {
	
	/*
	 * table comprising Orchestrator properties as name/value pairs
	 */
	private static String ORCHESTRATOR_PROPERTIES_TABLE = DbOrchestratorProperties.TABLE_NAME.pname();

	private static String COL_NAME = DbOrchestratorProperties.name.columnName();
	private static String COL_VALUE = DbOrchestratorProperties.value.columnName();
	
	private DuccLogger logger = null;
	private DbManager dbManager = null;

	private DuccId jobid = null;
	
	public DbOrchestratorProperties(DuccLogger duccLogger) throws Exception {
		init(duccLogger);
	}
	
	/*
	 * connect to DB
	 */
	private boolean init(String[] dburls) throws Exception {
		String methodName = "init";
		boolean ret = false;
		while (true) {
			try {
				dbManager = new DbManager(dburls, logger);
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
	 *   Orchestrator properties table
	 */
	protected static List<SimpleStatement> mkSchema() throws Exception {
		List<SimpleStatement> ret = new ArrayList<SimpleStatement>();

		StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS "
				+ ORCHESTRATOR_PROPERTIES_TABLE + " (");
		buf.append(DbUtil.mkSchema(DbOrchestratorProperties.values()));
		buf.append(")");
		ret.add(new SimpleStatement(buf.toString()));
		List<String> indexes = DbUtil.mkIndices(DbOrchestratorProperties.values(),
				ORCHESTRATOR_PROPERTIES_TABLE);
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
		try {
			List<SimpleStatement>specificationsSchema = mkSchema();
			DbHandle h = dbManager.open();
	        for ( SimpleStatement s : specificationsSchema ) {
	            logger.info(location, jobid, "EXECUTE STATEMENT:"+s.toString());
	            h.execute(s);
	        }
	        initPublicationSeqNo();
			initDuccWorkSeqNo();
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
		 this.logger = duccLogger;
		 String dbUrlsString = System.getProperty(DbManager.URL_PROPERTY);
	     String[] dbUrls = DbUtil.dbServersStringToArray(dbUrlsString);
	     init(dbUrls);
	}

	/**
	 * Add or update an Orchestrator property
	 */
	@Override
	public void upsert(String name, String value) throws Exception {
		String location = "upsert";
		try {
			String table = ORCHESTRATOR_PROPERTIES_TABLE;
			String c0 = COL_VALUE+"="+"'"+value+"'";
			String c1 = COL_NAME+"="+"'"+name+"'";
	        String cql = "UPDATE "+table+" SET "+c0+" WHERE "+c1;
	        logger.debug(location, jobid, cql);
	        DbHandle h = dbManager.open();
	        h.execute(cql);
		}
		catch(Exception e) {
			DuccId duccid = null;
			String text = "name="+name+" "+"value="+value;
			logger.error(location, duccid, text, e);
			throw e;
		}
	}
	
	/**
	 * Retrieve an Orchestrator property
	 */
	@Override
	public String fetch(String name) throws Exception {
		String location = "fetch";
		String value = null;
		try {
			String table = ORCHESTRATOR_PROPERTIES_TABLE;
			String c1 = COL_NAME+"="+"'"+name+"'";
	        String cql = "SELECT * FROM "+table+" WHERE "+c1;
	        logger.debug(location, jobid, cql);
	        DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        for ( Row r : rs ) {
	            String colValue = r.getString(COL_VALUE);
	            value = colValue;
	        }
		}
		catch(Exception e) {
			DuccId duccid = null;
			String text = "name="+name;
			logger.error(location, duccid, text, e);
			throw e;
		}
        return value;
	}

	public enum keys {
		publication_seqno,
		duccwork_seqno,
	}
	
	private void initPublicationSeqNo() {
		try {
			String value = fetch(keys.publication_seqno.name());
			Long.parseLong(value);
		}
		catch(Exception e) {
			setPublicationSeqNo(0);
		}
	}
	
	@Override
	public void setPublicationSeqNoIfLarger(long value) {
		long curr = value;
		long prev = getPublicationSeqNo();
		if(curr > prev) {
			setPublicationSeqNo(curr);
		}
	}
	
	@Override
	public void setPublicationSeqNo(long value) {
		String location = "setPublicationSeqNo";
		try {
			upsert(keys.publication_seqno.name(),""+value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}

	@Override
	public long getPublicationSeqNo() {
		String location = "getPublicationSeqNo";
		long retVal = 0;
		try {
			String value = fetch(keys.publication_seqno.name());
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

	//
	
	@Override
	public void setDuccWorkSeqNoIfLarger(long value) {
		String location = "setDuccWorkSeqNoIfLarger";
		long curr = value;
		long prev = getDuccWorkSeqNo();
		if(curr > prev) {
			setDuccWorkSeqNo(curr);
			logger.debug(location, jobid, curr);
		}
	}
	
	private void initDuccWorkSeqNo() {
		String location = "initDuccWorkSeqNo";
		long lval = 0;
		try {
			String value = fetch(keys.duccwork_seqno.name());
			lval = Long.parseLong(value);
		}
		catch(Exception e) {
			setDuccWorkSeqNo(0);
		}
		logger.debug(location, jobid, lval);
	}
	
	@Override
	public void setDuccWorkSeqNo(long value) {
		String location = "setDuccWorkSeqNo";
		try {
			upsert(keys.duccwork_seqno.name(),""+value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.debug(location, jobid, value);
	}

	@Override
	public long getDuccWorkSeqNo() {
		String location = "getDuccWorkSeqNo";
		long retVal = 0;
		try {
			String value = fetch(keys.duccwork_seqno.name());
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.debug(location, jobid, retVal);
		return retVal;
	}

	//////////
	
	@Override
	public long getNextPublicationSeqNo() {
		synchronized(DbOrchestratorProperties.class) {
			long retVal = getPublicationSeqNo()+1;
			setPublicationSeqNo(retVal);
			return retVal;
		}
	}

	@Override
	public long getNextDuccWorkSeqNo() {
		String location = "getNextDuccWorkSeqNo";
		synchronized(DbOrchestratorProperties.class) {
			long retVal = getDuccWorkSeqNo()+1;
			setDuccWorkSeqNo(retVal);
			logger.debug(location, jobid, retVal);
			return retVal;
		}
	}

}
