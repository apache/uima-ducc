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

public class DefaultCallback implements IDuccCallback {


  // pnum: 0,1 => AP   2 => JD   3,... => JP
  public void console(int pnum, String msg) {
    int jpid = pnum - 2;  // get number of JP
    String prefix;
    if (jpid < 0) {
      prefix = "";
    } else if (jpid == 0) {
      prefix = "[JD] ";
    } else {
      prefix = "[JP" + jpid + "] ";
    }
    // Only the AP has split streams
    if (pnum == 1) {
      System.err.println(prefix + msg);
    } else {
      System.out.println(prefix + msg);
    }
  }


  public void status(String msg) {
    System.out.println(msg);
  }

}


