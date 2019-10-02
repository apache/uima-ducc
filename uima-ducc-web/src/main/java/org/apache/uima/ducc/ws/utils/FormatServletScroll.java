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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

// ============================================================
// Creates a servlet response for the "Scroll" view
//============================================================

public class FormatServletScroll implements FormatServlet {
	
  private int counter;
  private JsonArray data;
  private JsonArray row;

  public FormatServletScroll() {
    data = new JsonArray();
    counter = 0;
  }
  
  public void startRow() {
    ++counter;
    row = new JsonArray();
  }
  
  public void addElemL(String value) {
    row.add(new JsonPrimitive(value));
  }
  
  public void addElemR(String value) {
    row.add(new JsonPrimitive(value));
  }
  
  public void addElemR(int value) {
    row.add(new JsonPrimitive(value));
  }
  
  public void addElemR(String value, long sortValue) {
    row.add(new JsonPrimitive(value));
  }
  
  public void pad(int n) {
    while(--n >= 0) {
      row.add(new JsonPrimitive(""));
    }
  }
  
  public void endRow() {
    data.add(row);
  }
  
  public int numRows() {
    return counter;
  }

  public String send(HttpServletResponse response) throws IOException {
    JsonObject jsonResponse = new JsonObject();
    jsonResponse.add("aaData", data);
    String json = jsonResponse.toString();
    response.getWriter().println(json);
    response.setContentType("application/json");
    return json;
  }


}
