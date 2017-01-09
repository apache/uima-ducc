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
package org.apache.uima.ducc.user.jd;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;

public class JdUserCasManager {

	private String CAS_INITIAL_HEAP_SIZE = JdUser.CAS_INITIAL_HEAP_SIZE;
	
	private ConcurrentLinkedQueue<CAS> recycledCasList = new ConcurrentLinkedQueue<CAS>();
	
	private TypeSystemDescription tsd = null;
	private TypePriorities tp = null;
	private FsIndexDescription[] fid;
	private Properties crProperties = new Properties();
	
	public JdUserCasManager(CollectionReader cr) {
		setTypeSystemDescription(cr.getProcessingResourceMetaData().getTypeSystem());
		setTypePriorities(cr.getProcessingResourceMetaData().getTypePriorities());
		setFsIndexDescription(cr.getProcessingResourceMetaData().getFsIndexes());
		initCrProperties();
	}
	
	public CAS getEmptyCas() throws ResourceInitializationException {
		CAS cas = recycledCasList.poll();		// Returns null if empty
		if (cas == null) {
			synchronized(CasCreationUtils.class) {
				cas = CasCreationUtils.createCas(tsd, tp, fid, crProperties);
			}
		}
		return cas;
	}
	
	public void recycle(CAS cas) {
		cas.reset();
		recycledCasList.add(cas);
	}
	
	private void setTypeSystemDescription(TypeSystemDescription value) {
		tsd = value;
	}

	public TypeSystemDescription getTypeSystemDescription() {
		return tsd;
	}
	
	private void setTypePriorities(TypePriorities value) {
		tp = value;
	}

	public TypePriorities getTypePriorities() {
		return tp;
	}
	
	private void setFsIndexDescription(FsIndexDescription[] value) {
		fid = value;
	}

	public FsIndexDescription[] getFsIndexDescription() {
		return fid;
	}
	
	private void initCrProperties() {
		// Change the heap size to reduce CAS size. The size here depends on
		// what we intend to
		// put into the CAS. If its just a pointer to data (like question id),
		// the size of the
		// heap can be reduced even more
		crProperties.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, CAS_INITIAL_HEAP_SIZE);
	}
}
