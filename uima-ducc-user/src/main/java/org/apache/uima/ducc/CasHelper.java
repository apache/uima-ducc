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
package org.apache.uima.ducc;

import java.util.Iterator;

import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.cas.TOP;

public class CasHelper {

	public static String getId(CAS cas) {
		String retVal = null;
		if (cas != null) {
			retVal = cas.getDocumentText();
			try {
				Iterator<TOP> fsIter = null;
				Workitem wi = null;
				if (cas.getJCas().getTypeSystem().getType(Workitem.class.getName()) != null) {
					fsIter = cas.getJCas().getJFSIndexRepository().getAllIndexedFS(Workitem.type);
				}
				if (fsIter != null && fsIter.hasNext()) {
					wi = (Workitem) fsIter.next();
				}
				if (wi != null) {
					String id = wi.getInputspec();
					if(id != null) {
						retVal = id;
					}
				}
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		return retVal;
	}
}
