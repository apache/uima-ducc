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

import org.apache.uima.ducc.common.persistence.IDbProperty;

/*
 * Interface to manage table of DUCC daemons & desired state.
 */
public interface IDbDaemonLifetime {
	
	public enum DaemonLifetimeProperties implements IDbProperty {
		TABLE_NAME {
			public String pname() {
				return "daemonLifetime";
			}

			public Type type() {
				return Type.String;
			}
		},
		// The order of the primary keys is important here as the Db assigns
		// semantics to the first key in a compound PK
		host { 
			public boolean isPrimaryKey() {
				return true;
			}
		},
		daemon {
			public boolean isPrimaryKey() {
				return true;
			}
		},
		state {
			
		},
		tod {
			public Type type() {
				return Type.Long;
			}
		},
		;
		
		public String pname() {
			return name();
		}

		public Type type() {
			return Type.String;
		}

		public boolean isPrimaryKey() {
			return false;
		}

		public boolean isPrivate() {
			return false;
		}

		public boolean isMeta() {
			return false;
		}

		public boolean isIndex() {
			return false;
		}

		public String columnName() {
			return pname();
		}
	}
	
	public enum Daemon { 
		Ag, 
		Or, 
		Pm,
		Rm, 
		Sm,
		Ws,
		Br,
		Db,
		;
		public String lcname() {
			return name().toLowerCase();
		}
	}
	
	public enum DesiredState { 
		Start, 
		Stop, 
		Quiesce,
	}
	
	public class DaemonInfo {
		public String host = null;
		public String daemon = null;
		public String state = null;
		public Long tod = null;
		public DaemonInfo(String host, String daemon, String state, Long tod) {
			this.host = host;
			this.daemon = daemon;
			this.state = state;
			this.tod = tod;
		}
	}
	
	public class DbResult {
		public static final Integer RC_Failure = DbDaemonLifetimeCommon.RC_Failure;
		public int rc = RC_Failure;
		public List<DaemonInfo> list = null;
	}
	
	public DbResult delete(String host, String daemon);
	public DbResult query(String host, String daemon);
	public DbResult quiesce(String host, String daemon, Long tod);
	public DbResult start(String host, String daemon, Long tod);
	public DbResult stop(String host, String daemon, Long tod);
	
}
