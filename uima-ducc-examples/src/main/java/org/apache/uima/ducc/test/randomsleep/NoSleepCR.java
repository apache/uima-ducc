/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.test.randomsleep;

import java.io.IOException;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

public class NoSleepCR extends CollectionReader_ImplBase {
		
	private volatile Logger logger;
    int qcount = 0;
	
	public void initialize() throws ResourceInitializationException {	
		super.initialize();

        logger = getLogger();
		logger.log(Level.INFO, "initialize");

		String strcount = ((String) getConfigParameterValue("count"));
        qcount = Integer.parseInt(strcount);
	}

    static int get_next_counter = 0;	
	
	public synchronized void getNext(CAS cas) throws IOException, CollectionException 
    {
        if ( get_next_counter < qcount ) {
            cas.reset();
            cas.setSofaDataString(""+get_next_counter, "text");
            logger.log(Level.INFO, " ****** getNext " + get_next_counter++);
        }
	}

	
	public void close() throws IOException {
		logger.log(Level.INFO, "close");
	}

	
	public Progress[] getProgress() {
		ProgressImpl[] retVal = new ProgressImpl[1];
		retVal[0] = new ProgressImpl(get_next_counter, qcount, "WorkItems");
		return retVal;
	}

	
	public boolean hasNext() throws IOException, CollectionException {
		return get_next_counter < qcount;
	}

}
