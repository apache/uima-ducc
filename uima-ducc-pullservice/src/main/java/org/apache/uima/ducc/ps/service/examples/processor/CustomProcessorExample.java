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
package org.apache.uima.ducc.ps.service.examples.processor;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class CustomProcessorExample implements IServiceProcessor {

  private Logger logger = UIMAFramework.getLogger(CustomProcessorExample.class);

  @Override
  public void initialize() throws ServiceInitializationException {
    logger.log(Level.INFO, "... initialize() called");
  }

  @Override
  public IProcessResult process(String serializedTask) {
    logger.log(Level.INFO, "... process() called");
    return new SuccessResultExample("");
  }

  @Override
  public void stop() {
    logger.log(Level.INFO, "... stop() called");

  }

  public void setScaleout(int howManyThreads) {
    logger.log(Level.INFO, "... setScaleout() called");
  }

  public int getScaleout() {
    logger.log(Level.INFO, "... getScaleout() called");
    return 1;
  }

  @Override
  public void setErrorHandlerWindow(int maxErrors, int windowSize) {
    logger.log(Level.INFO, "... setErrorHandlerWindow() called");

  }
}
