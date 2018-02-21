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
package org.apache.uima.ducc.common.persistence.or;

import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.utils.DuccLogger;

public interface IDbDuccWorks {

	enum DbDuccWorks implements IDbProperty {
		TABLE_NAME {
			public String pname() {
				return "duccworks";
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
		type { 
			// value = { "job", "reservation", "service", ... }
			public boolean isPrimaryKey() {
				return true;
			}
		},
		ducc_id {
			public Type type() {
				return Type.Long;
			}
			public boolean isPrimaryKey() {
				return true;
			}
		},
		specification {
			// json data = map: { user: Properties }, { system: Properties }
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

	/**
	 * Establish a logger and anything else the persistence may need.
	 * 
	 * @param logger
	 *            This is the logger to be used. It is usually the same logger
	 *            as the client of persistence, e.g. org.apache.uima.ducc.or.
	 *            The implementor is required to adjust itself to use this
	 *            logger to insure messages are logged into the right log.
	 */
	public void init(DuccLogger logger) throws Exception;

	/**
	 * create table(s)
	 */
	public void dbInit() throws Exception;
	
	/**
	 * insert or update specification
	 */
	public void upsertSpecification(String type, long id, ITypedProperties typedProperties)
			throws Exception;
	
	/**
	 * fetch specification
	 */
	public ITypedProperties fetchSpecification(String type, long id) throws Exception;
	
}
