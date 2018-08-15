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

package org.apache.uima.ducc.database.lifetime;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.uima.ducc.database.lifetime.IDbDaemonLifetime.Daemon;
import org.apache.uima.ducc.database.lifetime.IDbDaemonLifetime.DaemonInfo;
import org.apache.uima.ducc.database.lifetime.IDbDaemonLifetime.DbResult;

/*
 * Class comprising UI to manage table of DUCC daemons & desired state with respect to delete, query, quiesce, start, and stop.
 */
public class DbDaemonLifetimeUI {
	
	public static final Integer RC_Failure = DbDaemonLifetimeCommon.RC_Failure;
	public static final Integer RC_Success = DbDaemonLifetimeCommon.RC_Success;
	public static final Integer RC_Help = DbDaemonLifetimeCommon.RC_Help;
	
	private String __delete  = "--delete";
	private String __query   = "--query";
	private String __quiesce = "--quiesce";
	private String __start   = "--start";
	private String __stop    = "--stop";
	
	private String[] kw_list = { __delete, __query, __quiesce, __start, __stop };
	private String[] daemon_list = { Daemon.Ag.lcname(), Daemon.Or.lcname(), Daemon.Pm.lcname(), Daemon.Rm.lcname(), Daemon.Sm.lcname(), Daemon.Ws.lcname(), Daemon.Br.lcname(), Daemon.Db.lcname() };
	
	private String help_selector = "<"+__delete+"|"+__query+"|"+__quiesce+"|"+__start+"|"+__stop+">";
	private String help_host = "<host>";
	private String help_daemons = 
			"<"
			+Daemon.Ag.lcname()
			+"|"
			+Daemon.Or.lcname()
			+"|"
			+Daemon.Pm.lcname()
			+"|"
			+Daemon.Rm.lcname()
			+"|"
			+Daemon.Sm.lcname()
			+"|"
			+Daemon.Ws.lcname()
			+"|"
			+Daemon.Br.lcname()
			+"|"
			+Daemon.Db.lcname()
			+
			">"
			;
	
	private String message_specify = "specify one of "
			+__delete
			+", "
			+__query
			+", "
			+__quiesce
			+", "
			+__start
			+", "
			+__stop
			+"";

	private String help_msg = help_selector+" "+help_host+" "+help_daemons+"\n";
	
	private DbDaemonLifetime dbDaemonLifetime = null;
	
	private DbDaemonLifetimeUI() {
		Level level = Level.DEBUG;
		dbDaemonLifetime = new DbDaemonLifetime(level);
	}
	
	private void help() {
		System.out.println(help_msg);
		System.exit(RC_Help);
	}
	
	private boolean is_valid_host(String arg) {
		boolean retVal = false;
		if(arg.length() > 0) {
			retVal = true;
		}
		return retVal;
	}
	
	private boolean is_valid(String needle, String[] haystack) {
		boolean retVal = false;
		if(needle != null) {
			if(haystack != null) {
				for(String item : haystack) {
					if(needle.equalsIgnoreCase(item)) {
						retVal = true;
						break;
					}
				}
			}
		}
		return retVal;
	}
	
	private boolean is_valid_kw(String arg) {
		return is_valid(arg,kw_list);
	}
	
	private boolean is_valid_daemon(String arg) {
		return is_valid(arg,daemon_list);
	}
	
	private int delete(String host, String daemon) {
		DbResult dbResult = dbDaemonLifetime.delete(host,daemon);
		return dbResult.rc;
	}
	
	private int query(String host, String daemon) {
		DbResult dbResult = dbDaemonLifetime.query(host,daemon);
		List<DaemonInfo> list = dbResult.list;
		if(list != null) {
			for(DaemonInfo di : list) {
				System.out.println(di.host+"."+di.daemon+"="+di.state);
			}
		}
		return dbResult.rc;
	}
	
	private int quiesce(String host, String daemon, Long tod) {
		DbResult dbResult = dbDaemonLifetime.quiesce(host,daemon,tod);
		return dbResult.rc;
	}
	
	private int start(String host, String daemon, Long tod) {
		DbResult dbResult = dbDaemonLifetime.start(host,daemon,tod);
		return dbResult.rc;
	}
	
	private int stop(String host, String daemon, Long tod) {
		DbResult dbResult;
		if((host == null) || (daemon == null)) {
			dbResult = dbDaemonLifetime.query(host,daemon);
			if(dbResult.rc == RC_Success) {
				List<DaemonInfo> list = dbResult.list;
				if(list != null) {
					for(DaemonInfo di : list) {
						stop(di.host, di.daemon, tod);
					}
				}
			}
		}
		else {
			dbResult = dbDaemonLifetime.stop(host,daemon,tod);
		}
		return dbResult.rc;
	}
	
	private int mainline(String[] args) {
		int rc = RC_Failure;
		Long tod = System.currentTimeMillis();
		String kw = null;
		if(args.length > 0) {
			kw = DbDaemonLifetimeCommon.normalize_kw(args[0]);
		}
		String host = null;
		if(args.length > 1) {
			host = DbDaemonLifetimeCommon.normalize_host(args[1]);
		}
		String daemon = null;
		if(args.length > 2) {
			daemon = DbDaemonLifetimeCommon.normalize_daemon(args[2]);
		}
		//
		if(args.length < 1) {
			String message = "no arguments specified";
			System.out.println(message);
			help();
		}
		//
		if(args.length == 1) {
			if(!is_valid_kw(kw)) {
				System.out.println(message_specify);
				help();
			}
			if(kw.equalsIgnoreCase(__query)) {
				rc = query(host,daemon);
			}
			else if(kw.equalsIgnoreCase(__stop)) {
				rc = stop(host,daemon,tod);
			}
			else {
				String message = "specify host and daemon";
				System.out.println(message);
				help();
			}
		}
		//
		else if(args.length == 2) {
			if(!is_valid_kw(kw)) {
				System.out.println(message_specify);
				help();
			}
			if(!is_valid_host(host)) {
				help();
			}
			if(kw.equalsIgnoreCase(__query)) {
				rc = query(host,daemon);
			}
			else if(kw.equalsIgnoreCase(__stop)) {
				rc = stop(host,daemon,tod);
			}
			else {
				String message = "specify host and daemon";
				System.out.println(message);
				help();
			}
		}
		//
		else if(args.length == 3) {
			if(!is_valid_kw(kw)) {
				System.out.println(message_specify);
				help();
			}
			if(!is_valid_host(host)) {
				help();
			}
			if(!is_valid_daemon(daemon)) {
				help();
			}
			if(kw.equalsIgnoreCase(__query)) {
				rc = query(host,daemon);
			}
			else if(kw.equalsIgnoreCase(__delete)) {
				rc = delete(host,daemon);
			}
			else if(kw.equalsIgnoreCase(__quiesce)) {
				rc = quiesce(host,daemon,tod);
			}
			else if(kw.equalsIgnoreCase(__start)) {
				rc = start(host,daemon,tod);
			}
			else if(kw.equalsIgnoreCase(__stop)) {
				rc = stop(host,daemon,tod);
			}
			else {
				System.out.println(message_specify);
				help();
			}
		}
		//
		else {
			String message = "too many arguments";
			System.out.println(message);
			help();
		}
		return rc;
	}
	
	public static void main(String[] args) {
		int rc = RC_Failure;
		try {
			DbDaemonLifetimeUI instance = new DbDaemonLifetimeUI();
			rc = instance.mainline(args);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		System.exit(rc);
	}
}
