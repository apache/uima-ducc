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

// ============================================================
// Creates a servlet response for the "Classic" view
//============================================================

public class FormatServletClassic implements FormatServlet {
	
  private StringBuffer sb;
  private int counter;

  public FormatServletClassic() {
    sb = new StringBuffer();
    counter = 0;
  }
  
  public void startRow() {
    if ((counter++ % 2) > 0) {
      sb.append("<tr class=\"ducc-row-odd\">");
    } else {
      sb.append("<tr class=\"ducc-row-even\">");
    }
  }
  
  public void addElemL(String value) {
    sb.append("<td>");
    sb.append(value);
    sb.append("</td>");
  }
  
  public void addElemR(String value) {
    sb.append("<td align=\"right\">");
    sb.append(value);
    sb.append("</td>");
  }
  
  public void addElemR(int value) {
    sb.append("<td align=\"right\">");
    sb.append(value);
    sb.append("</td>");
  }
  
  public void addElemR(String value, long sortValue) {
    sb.append("<td ");
    sb.append("<td sorttable_customkey=\"");
    sb.append(sortValue);
    sb.append("\" align=\"right\">");
    sb.append(value);
    sb.append("</td>");
  }
  
  public void pad(int n) {
    while(--n >= 0) {
      sb.append("<td/>");
    }
  }
  
  public void endRow() {
    sb.append("</tr>");
  }
  
  public int numRows() {
    return counter;
  }
  
  public String send(HttpServletResponse response) throws IOException {
    response.getWriter().println(sb.toString());
    return sb.toString();
  }


}
