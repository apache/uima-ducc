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
package org.apache.uima.ducc.common.config;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ducc Predicate which should be placed in Camel timer routes to prevent
 * blast of messages flooding DUCC daemons. Its been observed that these
 * blasts occur although the exact reason is not yet understood. This predicate
 * detects how much time passed since the last time the prodicate was called.
 * If the interval is 1 sec or less, the guard returns false forcing the route
 * to end and dispose current message.
 *
 */
public class DuccBlastGuardPredicate implements Predicate  {
  private DateTime dt;
  DuccLogger logger;
  public DuccBlastGuardPredicate(DuccLogger logger) {
    this.logger = logger;
  }
  /**
   * Detects Camel blasts in timer routes. Every time the method is called
   * a check is made if the call came within a sec (or less) from the previous
   * one. 
   * 
   * Returns false if a message came in within a second of the previous one. Returns 
   * true otherwise.
   */
  public synchronized boolean matches(Exchange exchange) {
    String methodName="DuccNodeFilter.matches";
    boolean result = false;
    try {
      if ( dt == null ) {
        dt = new DateTime();
        return true;
      }
      Duration interval = new Duration(dt, new Instant());
      dt = new DateTime();
      //  check if filter called within an interval of 1 second or less. If so, we
      //  just detected a blast. Return false to invalidate the route and dispose the
      //  message
      if ( interval.isShorterThan(Duration.standardSeconds(1)) ) {
        //System.out.println("...... BlasTGuard ON ... Disposing Message .... Interval Since the Last Message:"+interval.toString());
        logger.warn(methodName, null,"...... BlasTGuard ON ... Disposing Message .... Interval Since the Last Message:"+interval.toString());
        return false;
      }
      result = true;
    } catch( Throwable e) {
      e.printStackTrace();
      logger.error(methodName, null, e, new Object[] {});
    }
    return result;
   }
}
