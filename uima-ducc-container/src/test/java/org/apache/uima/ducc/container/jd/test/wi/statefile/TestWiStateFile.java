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
package org.apache.uima.ducc.container.jd.test.wi.statefile;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateKeeper;
import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateReader;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateKeeper;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateReader;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.test.TestBase;
import org.junit.Before;
import org.junit.Test;

public class TestWiStateFile extends TestBase {
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getInstance();
    }
	
	@Test
	public void test_01() {
		try {
			File working = mkWorkingDir();
			String component = "JD.test";
			String directory = working.getAbsolutePath();
			debug(directory);
			IWorkItemStateKeeper wisk = new WorkItemStateKeeper(component, directory);
			//
			// int seqNo, String wiId, String node, String pid, String tid
			//
			wisk.start(0, "u.0", "n.0", "p.0", "t.0");
			//
			wisk.start(1, "u.1", "n.1", "p.1", "t.1");
			wisk.queued(1);
			//
			wisk.start(2, "u.2", "n.2", "p.2", "t.2");
			wisk.queued(2);
			wisk.operating(2);
			//
			wisk.start(3, "u.3", "n.3", "p.3", "t.3");
			wisk.queued(3);
			wisk.operating(3);
			wisk.ended(3);
			//
			wisk.start(4, "u.4", "n.4", "p.4", "t.4");
			wisk.queued(4);
			wisk.operating(4);
			wisk.error(4);
			//
			wisk.start(5, "u.5", "n.5", "p.5", "t.5");
			wisk.queued(5);
			wisk.operating(5);
			wisk.retry(5);
			//
			wisk.persist();
			//
			String user = "self";
			long version = 1;
			IWorkItemStateReader wisr = new WorkItemStateReader(component, directory, user, version);		
			int size = wisr.getMap().size();
			debug("size="+size);
			assertTrue(size > 0);
			//
			wisk.zip();
			//
			wisr = new WorkItemStateReader(component, directory, user, version);
			size = wisr.getMap().size();
			assertTrue(size > 0);
			//
			delete(working);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
}
