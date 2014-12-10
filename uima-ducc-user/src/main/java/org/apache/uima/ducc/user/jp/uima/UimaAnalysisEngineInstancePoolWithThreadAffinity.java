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
package org.apache.uima.ducc.user.jp.uima;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.uima.analysis_engine.AnalysisEngine;


public class UimaAnalysisEngineInstancePoolWithThreadAffinity {
	  private static final Class<?> CLASS_NAME = UimaAnalysisEngineInstancePoolWithThreadAffinity.class;

	  private volatile boolean destroyAEInstanceIfFree=false;
	  private Semaphore lock = new Semaphore(1);
	  
	  private Map<Long, AnalysisEngine> aeInstanceMap = new HashMap<Long,AnalysisEngine>();

	  public int size() {
	    return aeInstanceMap.size();
	  }

	  public void checkin(AnalysisEngine anAnalysisEngine) throws Exception {
		  try {
			  lock.acquireUninterruptibly();
			  // Call destroy() on AE on checkin if the process is in quiesce mode  
			  if ( destroyAEInstanceIfFree ) {
			    anAnalysisEngine.destroy();
			  } else {
		      aeInstanceMap.put(Thread.currentThread().getId(), anAnalysisEngine);
			  }
	 	  } catch( Exception e) {
			  e.printStackTrace();
			  throw e;
		  } finally {
			  lock.release();
		  }
	  }

	  public boolean exists() {
	    return aeInstanceMap.containsKey(Thread.currentThread().getId());
	  }

	  /**
	   * Pins each process thread to a specific and dedicated AE instance. All AE instances are managed
	   * in a HashMap with thread name as a key. AE instance is not removed from the HashMap before it
	   * is returned to the client.
	   * 
	   * @see org.apache.uima.aae.controller.AnalysisEngineInstancePool#checkout()
	   **/
	  public AnalysisEngine checkout() throws Exception {
		  try {
			  lock.acquireUninterruptibly();
			  if ( !exists() ) {
				  throw new RuntimeException("AE instance not found in AE pool. Most likely due to service quiescing");
			  }
		    // AEs are instantiated and initialized in the the main thread and placed in the temporary list.
		    // First time in the process() method, each thread will remove AE instance from the temporary
		    // list
		    // and place it in the permanent instanceMap. The key to the instanceMap is the thread name.
		    // Each
		    // thread will always process a CAS using its own and dedicated AE instance.
		    return (AnalysisEngine) aeInstanceMap.remove(Thread.currentThread().getId());

		  } catch( Exception e) {
			  throw e;
		  } finally {
			  lock.release();
		  }

		  
		  
	  }
	  /*
	   * (non-Javadoc)
	   * 
	   * @see org.apache.uima.aae.controller.AnalysisEngineInstancePool#destroy()
	   */
	  public void destroy() throws Exception {
		  //	set the flag so that any AE instance returned from PrimitiveController
		  //    will be destroyed. 
		  destroyAEInstanceIfFree = true;
	  }

}
