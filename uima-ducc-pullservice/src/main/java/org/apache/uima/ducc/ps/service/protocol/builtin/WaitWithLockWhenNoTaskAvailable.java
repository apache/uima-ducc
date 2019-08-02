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

package org.apache.uima.ducc.ps.service.protocol.builtin;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class WaitWithLockWhenNoTaskAvailable implements INoTaskAvailableStrategy {
  private int waitTime;

  private final ReentrantLock lock = new ReentrantLock();

  Logger logger = UIMAFramework.getLogger(WaitWithLockWhenNoTaskAvailable.class);

  public WaitWithLockWhenNoTaskAvailable(int waitTimeInMillis) {
    this.waitTime = waitTimeInMillis;
    logger.log(Level.INFO, ">>>>>>>> Service Wait Time For Task:" + waitTimeInMillis + " ms");
  }

  /**
   * This methods is called when a service is stopping. There is no need to wait. We want to stop as
   * soon as possible so we just interrupt the lock which might be blocking in await()
   */
  @Override
  public void interrupt() {
    lock.unlock();
  }

  @Override
  public void handleNoTaskSupplied() {
    Condition waitAwhileCondition = lock.newCondition();
    try {
      lock.lock();
      // wait only if wait time > 0. Indefinite wait is not supported. If waitTime=0, it means no
      // wait
      if (waitTime > 0) {
        try {
          waitAwhileCondition.await(waitTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          System.out.println(
                  "DefaultNoTaskAvailableStrategy.handleNoTaskSupplied() - Waiting interrupted "
                          + " Thread:" + Thread.currentThread().getId());
        }
      }
    } finally {
      lock.unlock();

    }
  }

  @Override
  public long getWaitTimeInMillis() {
    return waitTime;
  }

}
