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

package org.apache.uima.ducc.ps.service.errors.builtin;

import java.util.Arrays;

import org.apache.uima.ducc.ps.service.IServiceComponent;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.metrics.IWindowStats;

public class WindowBasedErrorHandler implements IServiceErrorHandler {

  private int errorThreshold = 1;

  private int windowSize = 1;

  private long errorCount = 0;

  private long errorSequences[];

  public WindowBasedErrorHandler withMaxFrameworkErrors(int maxFrameworkError) {
    this.errorThreshold = maxFrameworkError;
    return this;
  }

  public WindowBasedErrorHandler withProcessErrorWindow(int errorWindow) {
    this.windowSize = errorWindow;
    return this;
  }

  public WindowBasedErrorHandler build() {
    if (windowSize >= errorThreshold && errorThreshold > 1) {
      errorSequences = new long[errorThreshold - 1];
      Arrays.fill(errorSequences, -windowSize);
    }
    return this;
  }

  public boolean exceededErrorWindow(long taskCount) {
    if (errorThreshold == 0) {
      return false;
    }
    ++errorCount;

    // If no window check if total errors have REACHED the threshold
    if (errorSequences == null) {
      return (errorCount >= errorThreshold);
    }
    // Insert in array by replacing one that is outside the window.
    int i = errorThreshold - 1;
    while (--i >= 0) {
      if (errorSequences[i] <= taskCount - windowSize) {
        errorSequences[i] = taskCount;
        return false;
      }
    }
    // If insert fails then have reached threshold.
    // Should not be called again after returning true as may return false!
    // But may be called again if no action specified, but then it doesn't matter.
    return true;
  }

  @Override
  public Action handleProcessError(Exception e, IServiceComponent source, IWindowStats stats) {
    Action action = Action.CONTINUE;
    if (exceededErrorWindow(stats.getSuccessCount())) {
      action = Action.TERMINATE;
    }
    return action;
  }

  @Override
  public Action handle(Exception e, IServiceComponent source) {
    return Action.TERMINATE;
  }

}
