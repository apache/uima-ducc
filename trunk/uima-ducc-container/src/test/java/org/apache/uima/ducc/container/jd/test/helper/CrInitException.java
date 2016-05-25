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
package org.apache.uima.ducc.container.jd.test.helper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

public class CrInitException extends CollectionReader_ImplBase {

	private int casLimit = 100;
	private AtomicInteger casCounter = new AtomicInteger(0);
	
	@Override 
	public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> aAdditionalParams)
	          throws ResourceInitializationException {
		throw new RuntimeException();
	}
	
	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		aCAS.reset();
		int item = casCounter.incrementAndGet();
		if(item <= casLimit) {
			aCAS.setSofaDataString(""+item, "text");
		}
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		boolean retVal = false;
		if(casCounter.get() < casLimit) {
			retVal = true;
		}
		return retVal;
	}

	@Override
	public Progress[] getProgress() {
		ProgressImpl[] retVal = new ProgressImpl[1];
		retVal[0] = new ProgressImpl(casCounter.get(), casLimit, "CASes");
		return retVal;
	}

	@Override
	public void close() throws IOException {
	}

}
