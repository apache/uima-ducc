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
 * Class to maintain and query database table of "autostart" daemons.
 * 
 * > use --start to add host + daemon name to table.
 * > use --stop to remove host + daemon name from table.
 * > use --query to determine if host + daemon name resides in table.
 * 
 */

package org.apache.uima.ducc.database.lifetime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
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
 * Class to manage table of DUCC daemons & desired state with respect to delete, query, quiesce, start, and stop.
 */
public class DbDaemonLifetime implements IDbDaemonLifetime {
	
	private DuccLogger logger = null;
	private DuccId id = null;
	
	public static final Integer RC_Success = DbDaemonLifetimeCommon.RC_Success;
	
	private static DbManager dbManager = null;
	
	private static String KEYSPACE = "DUCC";
	private static String DAEMON_LIFETIME_TABLE = DaemonLifetimeProperties.TABLE_NAME.pname();
	
	private static String key_host = DaemonLifetimeProperties.host.name();
	private static String key_daemon = DaemonLifetimeProperties.daemon.name();
	private static String key_state = DaemonLifetimeProperties.state.name();
	private static String key_tod = DaemonLifetimeProperties.tod.name();
	
	public DbDaemonLifetime() {
		logger = DuccLogger.getLogger(DbDaemonLifetime.class);
		init();
	}
	
	public DbDaemonLifetime(Level level) {
		logger = DuccLogger.getLogger(DbDaemonLifetime.class);
		logger.setLevel(level);
		init();
	}
	
	public DbDaemonLifetime(DuccLogger ducclogger) {
		logger = ducclogger;
		init();
	}
	
	private List<SimpleStatement> db_mkSchema() throws Exception {
		List<SimpleStatement> ret = new ArrayList<SimpleStatement>();
		StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + DAEMON_LIFETIME_TABLE + " (");
		buf.append(DbUtil.mkSchema(DaemonLifetimeProperties.values()));
		buf.append(")");
		ret.add(new SimpleStatement(buf.toString()));
		List<String> indexes = DbUtil.mkIndices(DaemonLifetimeProperties.values(), DAEMON_LIFETIME_TABLE);
		for (String s : indexes) {
			ret.add(new SimpleStatement(s));
		}
		return ret;
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
		return retVal;
	}
	
	private void shutdown() {
		dbManager.shutdown();
	}
	
	private DbResult db_upsert(String host, String daemon, String state, Long tod) {
		String mn = "db_upsert";
		DbResult dbResult = new DbResult();
		try {
			DbHandle h = dbManager.open();
			PreparedStatement ps = h.prepare("INSERT INTO " + DAEMON_LIFETIME_TABLE + " (host, daemon, state, tod) values(?, ?, ?, ?);");
			logger.debug(mn, id, "EXECUTE STATEMENT:"+ps.toString()+" ("+host+","+daemon+","+state+","+tod+")");
			logger.debug(mn, id, host, daemon, state, tod);
			ResultSet rs = h.execute(ps, host, daemon, state, tod);
			for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_host+"="+row.getString(key_host));
	            	sb.append(" ");
	            	sb.append(key_daemon+"="+row.getString(key_daemon));
	            	sb.append(" ");
	            	sb.append(key_state+"="+row.getString(key_state));
	            	sb.append(" ");
	            	sb.append(key_tod+"="+row.getLong(key_tod));
	            	logger.debug(mn, id, sb.toString());
	    		}
	    	}
			dbResult.rc = RC_Success;
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private DbResult db_update(String host, String daemon, String state, Long tod) {
		String mn = "db_update";
		DbResult dbResult = new DbResult();
		try {
			DbHandle h = dbManager.open();
			PreparedStatement ps;
			ResultSet rs;
			if(host.equals("*")) {
				ps = h.prepare("UPDATE " + DAEMON_LIFETIME_TABLE + " SET state=?, tod=? WHERE daemon=? IF EXISTS;");
				logger.debug(mn, id, "EXECUTE STATEMENT:"+ps.toString()+" ("+state+","+tod+","+daemon+")");
				logger.debug(mn, id, daemon, state, tod);
				rs = h.execute(ps, state, tod, daemon);
			}
			else {
				ps = h.prepare("UPDATE " + DAEMON_LIFETIME_TABLE + " SET state=?, tod=? WHERE host=? AND daemon=? IF EXISTS;");
				logger.debug(mn, id, "EXECUTE STATEMENT:"+ps.toString()+" ("+state+","+tod+","+host+","+daemon+")");
				logger.debug(mn, id, host, daemon, state, tod);
				rs = h.execute(ps, state, tod, host, daemon);
			}
			for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_host+"="+row.getString(key_host));
	            	sb.append(" ");
	            	sb.append(key_daemon+"="+row.getString(key_daemon));
	            	sb.append(" ");
	            	sb.append(key_state+"="+row.getString(key_state));
	            	sb.append(" ");
	            	sb.append(key_tod+"="+row.getLong(key_tod));
	            	logger.debug(mn, id, sb.toString());
	            	System.out.println(sb.toString());
	    		}
	    	}
			dbResult.rc = RC_Success;
		}
		catch(Exception e) {
			logger.error(mn, id, e);
			e.printStackTrace();
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private DbResult db_delete(String host, String daemon) {
		String mn = "db_delete";
		DbResult dbResult = new DbResult();
		try {
			String w_host = key_host+"="+"'"+host+"'";
			String w_daemon = key_daemon+"="+"'"+daemon+"'";
			String w_clause = "WHERE "+w_host+" AND "+w_daemon;
			String cql = "DELETE FROM "+KEYSPACE+"."+DAEMON_LIFETIME_TABLE+" "+w_clause+";";
			logger.debug(mn, id, "EXECUTE STATEMENT:"+cql);
			DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        for ( Row row : rs ) {
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_host+"="+row.getString(key_host));
	            	sb.append(" ");
	            	sb.append(key_daemon+"="+row.getString(key_daemon));
	            	sb.append(" ");
	            	sb.append(key_tod+"="+row.getLong(key_tod));
	            	logger.debug(mn, id, sb.toString());
	    		}
	    	}
	        dbResult.rc = RC_Success;
		}
		catch(InvalidQueryException e) {
			if(e.getMessage().equals("unconfigured table daemonlifetime")) {
				logger.debug(mn, id, e);
			}
			else {
				logger.error(mn, id, e);
			}
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private void add_kw_where(StringBuffer sb, String host, String daemon) {
		if(host != null) {
			sb.append("WHERE");
			sb.append(" ");
		}
		else if(daemon != null) {
			sb.append("WHERE");
			sb.append(" ");
		}
	}
	
	private void add_host(StringBuffer sb, String host) {
		if(host != null) {
			sb.append("host = ");
			sb.append("'");
			sb.append(host);
			sb.append("'");
			sb.append(" ");
		}
	}
	
	private void add_kw_and(StringBuffer sb, String host, String daemon) {
		if(host != null) {
			if(daemon != null) {
				sb.append("AND");
				sb.append(" ");
			}
		}
	}
	
	private void add_daemon(StringBuffer sb, String daemon) {
		if(daemon != null) {
			sb.append("daemon = ");
			sb.append("'");
			sb.append(daemon);
			sb.append("'");
			sb.append(" ");
		}
	}
	
	private String get_where_clause(String host, String daemon) {
		StringBuffer where_clause = new StringBuffer();
		add_kw_where(where_clause,host,daemon);
		add_host(where_clause,host);
		add_kw_and(where_clause,host,daemon);
		add_daemon(where_clause,daemon);
		String retVal = where_clause.toString().trim();
		return retVal;
	}
	
	private DbResult db_query(String host, String daemon) {
		String mn = "db_query";
		DbResult dbResult = new DbResult();
		List<DaemonInfo> list = new ArrayList<DaemonInfo>();
		try {
			String where_clause = get_where_clause(host,daemon);
			String cql = "SELECT * FROM "+KEYSPACE+"."+DAEMON_LIFETIME_TABLE+" "+where_clause+";";
			logger.debug(mn, id, cql);
			DbHandle h = dbManager.open();
	        ResultSet rs = h.execute(cql);
	        Iterator<Row> iterator = rs.iterator();
	        int rows = 0;
	        while ( iterator.hasNext()) {
	        	rows = rows + 1;
	        	Row row = iterator.next();
	    		int width = row.getColumnDefinitions().size();
	    		if(width > 1) {
	    			DaemonInfo daemonInfo = new DaemonInfo(
	    					row.getString(key_host),
	    					row.getString(key_daemon),
	    					row.getString(key_state),
	    					row.getLong(key_tod)
	    					);
	    			list.add(daemonInfo);
	        		StringBuffer sb = new StringBuffer();
	            	sb.append(key_host+"="+daemonInfo.host);
	            	sb.append(" ");
	            	sb.append(key_daemon+"="+daemonInfo.daemon);
	            	sb.append(" ");
	            	sb.append(key_state+"="+daemonInfo.state);
	            	sb.append(" ");
	            	sb.append(key_tod+"="+daemonInfo.tod);
	            	logger.debug(mn, id, sb.toString());
	    		}
	    	}
	        dbResult.list = list;
	        dbResult.rc = RC_Success;
		}
		catch(InvalidQueryException e) {
			if(e.getMessage().equals("unconfigured table daemonlifetime")) {
				logger.debug(mn, id, e);
			}
			else {
				logger.error(mn, id, e);
			}
		}
		catch(Exception e) {
			logger.error(mn, id, e);
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private DbResult db_quiesce(String host, String daemon, Long tod) {
		String mn = "db_quiesce";
		DbResult dbResult = new DbResult();
		if(init()) {
			if(db_init()) {
				dbResult = db_update(host, daemon, DesiredState.Quiesce.name(), tod);
			}
			shutdown();
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private DbResult db_start(String host, String daemon, Long tod) {
		String mn = "db_start";
		DbResult dbResult = new DbResult();
		if(init()) {
			if(db_init()) {
				dbResult = db_upsert(host, daemon, DesiredState.Start.name(), tod);
			}
			shutdown();
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	private DbResult db_stop(String host, String daemon, Long tod) {
		String mn = "db_stop";
		DbResult dbResult = new DbResult();
		if(init()) {
			if(db_init()) {
				dbResult = db_update(host, daemon, DesiredState.Stop.name(), tod);
			}
			shutdown();
		}
		logger.debug(mn, id, dbResult.rc);
		return dbResult;
	}
	
	//

	@Override
	public DbResult delete(String host, String daemon) {
		DbResult dbResult = new DbResult();
		if(init()) {
			dbResult = db_delete(host, daemon);
			shutdown();
		}
		return dbResult;
	}
	
	@Override
	public DbResult query(String host, String daemon) {
		DbResult dbResult = new DbResult();
		if(init()) {
			dbResult = db_query(host, daemon);
			shutdown();
		}
		return dbResult;
	}

	@Override
	public DbResult quiesce(String host, String daemon, Long tod) {
		DbResult dbResult = new DbResult();
		if(init()) {
			dbResult = db_quiesce(host, daemon, tod);
			shutdown();
		}
		return dbResult;
	}
	
	@Override
	public DbResult start(String host, String daemon, Long tod) {
		DbResult dbResult = new DbResult();
		if(init()) {
			dbResult = db_start(host, daemon, tod);
			shutdown();
		}
		return dbResult;
	}
	
	@Override
	public DbResult stop(String host, String daemon, Long tod) {
		DbResult dbResult = new DbResult();
		if(init()) {
			dbResult = db_stop(host, daemon, tod);
			shutdown();
		}
		return dbResult;
	}

}