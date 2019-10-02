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
package org.apache.uima.ducc.ws.utils;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

//============================================================
// Interface used when creating a servlet response 
// for the "Scroll" or "Classic" view
//============================================================

public interface FormatServlet {

  public void startRow();

  public void addElemL(String value);
  
  public void addElemR(String value);
  
  public void addElemR(int value);
  
  public void addElemR(String value, long sortValue);
  
  public void pad(int n);
  
  public void endRow();
  
  public int numRows();
  
  public String send(HttpServletResponse response) throws IOException;
}
