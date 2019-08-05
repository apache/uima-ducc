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

/**
 * Class to maintain and query database table of login userids.
 */

package org.apache.uima.ducc.database.login;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.db.DbHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbHandle;
import org.apache.uima.ducc.database.DbManager;
import org.apache.uima.ducc.database.DbUtil;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/*
 * Class to manage table of user logins
 */
public class DbUserLogin implements IDbUserLogin {
	
	private static String KEYSPACE = "DUCC";
	private static String USER_LOGIN_TABLE = UserLoginProperties.TABLE_NAME.pname();
	
	private static String key_name = UserLoginProperties.name.name();
	private static String key_validationId = UserLoginProperties.validationId.name();

	private DuccLogger logger = null;
	private DuccId id = null;
	
	private DbManager dbManager = null;

	public DbUserLogin(DuccLogger logger) {
		this.logger = logger;
		init();
	}
	
	private boolean init() {
		String mn = "init";
		boolean retVal = false;
		try {
			String[] dbUrls = DbHelper.getHostList();
			StringBuffer sb = new StringBuffer();
			for(String dbUrl : dbUrls) {
				sb.append(dbUrl+" ");
			}
			logger.debug(mn, id, dbUrls.length+" "+sb.toString());
			retVal = init(dbUrls);
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		db_init();
		return retVal;
	}
	
	private boolean init(String[] dburls) throws Exception {
		String mn = "init";
		boolean retVal = false;
		try {
			dbManager = new DbManager(dburls, logger);
			dbManager.init();
			retVal = true;
		} catch (Exception e) {
			logger.error(mn, id, "Errors contacting database.  No connetion made.");
			logger.error(mn, id, e);
		}
		return retVal;
	}
	
	private boolean db_init() {
		String mn = "db_init";
		boolean retVal = false;
		try {
			List<SimpleStatement>specificationsSchema = db_mkSchema();
			DbHandle h = dbManager.open();
	        for ( SimpleStatement s : specificationsSchema ) {
	            logger.debug(mn, id, "EXECUTE STATEMENT:"+s.toString());
	            h.execute(s);
	        }
	        retVal = true;
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		return retVal;
	}
	
	private List<SimpleStatement> db_mkSchema() throws Exception {
		List<SimpleStatement> ret = new ArrayList<SimpleStatement>();
		StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + USER_LOGIN_TABLE + " (");
		buf.append(DbUtil.mkSchema(UserLoginProperties.values()));
		buf.append(")");
		ret.add(new SimpleStatement(buf.toString()));
		List<String> indexes = DbUtil.mkIndices(UserLoginProperties.values(), USER_LOGIN_TABLE);
		for (String s : indexes) {
			ret.add(new SimpleStatement(s));
		}
		return ret;
	}
	
	@Override
	public boolean addOrReplace(String name, String validationId) {
		String mn = "addOrReplace";
		boolean retVal = false;
		try {
			DbHandle h = dbManager.open();
			PreparedStatement ps = h.prepare("INSERT INTO " + USER_LOGIN_TABLE + " (name, validationId) values(?, ?);");
			logger.debug(mn, id, "EXECUTE STATEMENT:"+ps.toString()+" ("+name+","+validationId+")");
			logger.debug(mn, id, name, validationId);
			ResultSet rs = h.execute(ps, name, validationId);
			for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_name+"="+row.getString(key_name));
	            	sb.append(" ");
	            	sb.append(key_validationId+"="+row.getString(key_validationId));
	            	logger.debug(mn, id, sb.toString());
	    		}
	    	}
			retVal = true;
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, retVal);
		return retVal;
	}

	@Override
	public boolean delete(String name) {
		String mn = "delete";
		boolean retVal = false;
		try {
			String w_name = key_name+"="+"'"+name+"'";
			String w_clause = "WHERE "+w_name;
			String cql = "DELETE FROM "+KEYSPACE+"."+USER_LOGIN_TABLE+" "+w_clause+";";
			logger.debug(mn, id, "EXECUTE STATEMENT:"+cql);
			DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_name+"="+row.getString(key_name));
	            	sb.append(" ");
	            	sb.append(key_validationId+"="+row.getString(key_validationId));
	            	logger.debug(mn, id, sb.toString());
	    		}
	    	}
	        retVal = true;
		}
		catch(InvalidQueryException e) {
			if(e.getMessage().equals("unconfigured table")) {
				logger.debug(mn, id, e);
			}
			else {
				logger.error(mn, id, e);
			}
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, retVal);
		return retVal;
	}

	@Override
	public String fetch(String name) {
		String mn = "fetch";
		String retVal = null;
		try {
			String w_name = key_name+"="+"'"+name+"'";
			String w_clause = "WHERE "+w_name;
			String cql = "SELECT * FROM "+KEYSPACE+"."+USER_LOGIN_TABLE+" "+w_clause+";";
			logger.debug(mn, id, "EXECUTE STATEMENT:"+cql);
			DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_name+"="+row.getString(key_name));
	            	sb.append(" ");
	            	sb.append(key_validationId+"="+row.getString(key_validationId));
	            	logger.debug(mn, id, sb.toString());
	            	retVal = row.getString(key_validationId);
	    		}
	    	}
		}
		catch(InvalidQueryException e) {
			if(e.getMessage().equals("unconfigured table")) {
				logger.debug(mn, id, e);
			}
			else {
				logger.error(mn, id, e);
			}
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, name, retVal);
		return retVal;
	}
	
}