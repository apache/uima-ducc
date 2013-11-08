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
package org.apache.uima.ducc.cli;

public interface IDuccCallback {

  /**
   * This method is called by relevant parts of the API with messages redirected from the remote
   * process.
   * 
   * @param pnum
   *          This is the callback number for the remote process e.g. 1 is assigned to the first
   *          process to call back
   * @param msg
   *          This is the logged message.
   */
  public void console(int pnum, String msg);

  /**
   * This method is called by relevant parts of the API with messages related to the status of the
   * submitted request.
   * 
   * @param msg
   *          This is the logged message.
   */
  public void status(String msg);

}
