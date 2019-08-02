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

package org.apache.uima.ducc.ps;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;
import org.apache.uima.util.Level;

/**
 * Factory for creating process threads.
 * 
 *
 */
public class ServiceThreadFactory implements ThreadFactory {
  private static final String THREAD_POOL = "[ServiceThreadPool ";

  private boolean isDaemon = false;

  private String threadNamePrefix = null;

  public static AtomicInteger poolIdGenerator = new AtomicInteger(1);

  @Override
  public Thread newThread(final Runnable r) {

    Thread newThread = null;
    try {
      newThread = new Thread(new Runnable() {
        public void run() {
          if (threadNamePrefix == null) {
            threadNamePrefix = THREAD_POOL + poolIdGenerator + "] " + " Process Thread";
          }
          Thread.currentThread().setName(threadNamePrefix + " - " + Thread.currentThread().getId());
          try {

            // Call given Worker (Runnable) run() method and block. This call blocks until the
            // TaskExecutor is terminated.
            r.run();

            UIMAFramework.getLogger().log(Level.INFO, "Thread " + Thread.currentThread().getName()
                    + " [" + Thread.currentThread().getId() + "] Terminating");
          } catch (Throwable e) {
            throw e;
          }
        }
      });
    } catch (Exception e) {
      UIMAFramework.getLogger().log(Level.WARNING, "", e);
    }
    newThread.setDaemon(isDaemon);
    return newThread;
  }

}
