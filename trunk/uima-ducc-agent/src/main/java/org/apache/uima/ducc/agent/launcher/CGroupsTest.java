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
package org.apache.uima.ducc.agent.launcher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;


public class CGroupsTest {
	 public static DuccLogger logger = DuccLogger.getLogger(NodeAgent.class, "CGroupsTest");
	 CGroupsManager cgroupsManager = null;
	 DuccIdFactory idFactory = null;
	 Lock lock = new ReentrantLock();
	 
	public static void main(String[] args) {
		try {
			CGroupsTest tester = new CGroupsTest();
			tester.initialize();
			if ( args.length > 1) {
				// run concurrent threads 
				tester.run(Long.parseLong(args[0]), true);
			} else {
				// run sequentially 
				tester.run(Long.parseLong(args[0]), false);
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	public void run(long howMany, boolean concurrent) {
		try {
			CGroupsTest tester = new CGroupsTest();
			tester.initialize();
			ExecutorService executor = Executors.newCachedThreadPool();
			// more than 1 arg to this program = concurrent
			if ( concurrent ) {
				for (int i = 0; i < howMany; i++) {
					WorkerThread t = new WorkerThread();
					executor.execute(t);
					// if the wait below is removed, cgroup creation fails
					synchronized(t) {
						// NOTE: waiting for 100ms seems to make cgcreate working.
						// Tested 10ms and got a failure to create cgroup. Weird.
						t.wait(100);
					}
				}
			} else {
				for (int i = 0; i < howMany; i++) {
					WorkerThread t = new WorkerThread();
					Future<?> f = executor.submit(t);
					f.get();

				}
			}
			executor.shutdownNow();
		} catch( Exception e) {
			e.printStackTrace();
		}
		
	}
	public void initialize() throws Exception {

		idFactory = new DuccIdFactory(null,null);
		String cgroupsUtilsDirs = System.getProperty("ducc.agent.launcher.cgroups.utils.dir");
      	String cgUtilsPath=null;
      	if (cgroupsUtilsDirs == null) {
        	cgUtilsPath = "/bin";  // default
        } 
        // get the top level cgroup folder from ducc.properties. If
        // not defined, use /cgroup/ducc as default
        String cgroupsBaseDir = System.getProperty("ducc.agent.launcher.cgroups.basedir");
        if (cgroupsBaseDir == null) {
          cgroupsBaseDir = "/cgroup/ducc";
        }
        String cgroupsSubsystems = "memory,cpu";
		long maxTimeToWaitForProcessToStop = 60000; // default 1 minute

		cgroupsManager = 
				new CGroupsManager(cgUtilsPath, cgroupsBaseDir, cgroupsSubsystems, logger, maxTimeToWaitForProcessToStop);

		
	}
	
	public class WorkerThread implements Runnable {
		public WorkerThread() {
			
		}
	public void run() {
		try {
			String containerId;
			lock.lock();
			containerId = idFactory.next().toString()+"."+idFactory.next().toString();
			
			System.out.println(">>>> Thread::"+Thread.currentThread().getId()+" creating cgroup with id:"+containerId);
			if ( !cgroupsManager.createContainer(containerId, System.getProperty("user.name"), 
					cgroupsManager.getUserGroupName(System.getProperty("user.name")),
					true) ) {
				System.out.println("Thread::"+Thread.currentThread().getId()+" Failure to create cgroup with id:"+containerId);
				System.exit(-1);
				
			} else {
				if ( cgroupsManager.cgroupExists(cgroupsManager.getDuccCGroupBaseDir() + "/" + containerId) ) {
					System.out.println("Thread::"+Thread.currentThread().getId()+" Success creating cgroup with id:"+containerId);
				
				cgroupsManager.setContainerSwappiness(containerId, cgroupsManager.getDuccUid(), true, 10);
				} else {
					System.out.println("Failed to validate existance of cgroup with id:"+containerId);
					System.exit(-1);
				}
			}
			cgroupsManager.destroyContainer(containerId, cgroupsManager.getDuccUid(), NodeAgent.SIGTERM);
			System.out.println("Cgroup "+containerId+" Removed");
		} catch( Exception e ) {
			e.printStackTrace();
			//System.exit(-1);
		} finally {
			lock.unlock();
		}
	}
	}
}
