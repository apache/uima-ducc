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
package org.apache.uima.ducc.transport.event.common;

public class Rationale implements IRationale {
	
	private static final long serialVersionUID = 1L;
	
	private String text = null;
	
	private static final String unspecified = "unspecified";
	
	public Rationale() {
		setText(unspecified);
	}
	
	public Rationale(String text) {
		setText(text);
	}
	
	public String getText() {
		return text;
	}
	
	public String getTextQuoted() {
		String retVal = "";
		if(text != null) {
			retVal = text.trim();
			retVal = retVal.replace("\"", "");
			if(!retVal.startsWith("\"")) {
				retVal = "\""+retVal+"\"";
			}
		}
		return retVal;
	}
	
	private void setText(String text) {
		this.text = text;
	}
	
	
	public String toString() {
		return getText();
	}

	
	public boolean isSpecified() {
		return !isUnspecified();
	}
	
	
	public boolean isUnspecified() {
		boolean retVal = false;
		if(text == null) {
			retVal = true;
		}
		else if(text.equalsIgnoreCase(unspecified)) {
			retVal = true;
		}
		return retVal;
	}
	
}
