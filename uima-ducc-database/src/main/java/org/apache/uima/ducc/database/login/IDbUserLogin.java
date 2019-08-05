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
package org.apache.uima.ducc.database.login;

import org.apache.uima.ducc.common.persistence.IDbProperty;

/*
 * Interface to manage table of DUCC logged-in users
 */
public interface IDbUserLogin {
	
	public enum UserLoginProperties implements IDbProperty {
		TABLE_NAME {
			public String pname() {
				return "UserLogin";
			}

			public Type type() {
				return Type.String;
			}
			
			public boolean isPrivate() {
				return true;
			}

			public boolean isMeta() {
				return true;
			}
		},
		// The order of the primary keys is important here as the Db assigns
		// semantics to the first key in a compound PK
		name { 
			public boolean isPrimaryKey() {
				return true;
			}
		},
		validationId {
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
	
	public boolean addOrReplace(String name, String validationValue);
	public boolean delete(String name);
	public String fetch(String name);
	
}
