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
package org.apache.uima.ducc.common;

import java.io.Serializable;

public class SizeBytes implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public static long KB = 1024;
	public static long MB = 1024*KB;
	public static long GB = 1024*MB;
	public static long TB = 1024*MB;
	
	public static enum Type { TBytes, GBytes, MBytes, KBytes, Bytes };
	
	public static Type getType(String value) {
		Type retVal = Type.Bytes;
		if(value != null) {
			if(value.equalsIgnoreCase("TB")) {
				retVal = Type.TBytes;
			}
			else if(value.equalsIgnoreCase("GB")) {
				retVal = Type.GBytes;
			}
			else if(value.equalsIgnoreCase("MB")) {
				retVal = Type.MBytes;
			}
			else if(value.equalsIgnoreCase("KB")) {
				retVal = Type.KBytes;
			}
		}
		return retVal;
	}
	private long value;
	
	public SizeBytes(Type type, long value) {
		initialize(type, value);
	}
	
	public SizeBytes(String units, long value) {
		initialize(getType(units), value);
	}
	
	private void initialize(Type type, long value) {
		switch(type) {
		case TBytes:
			setValue(TB*value);
			break;
		case GBytes:
			setValue(GB*value);
			break;
		case MBytes:
			setValue(MB*value);
			break;
		case KBytes:
			setValue(KB*value);
			break;
		case Bytes:
			setValue(value);
			break;
		}
	}
	
	public long getBytes() {
		return value;
	}
	
	public long getKBytes() {
		return value/KB;
	}
	
	public long getMBytes() {
		return value/MB;
	}
	
	public long getGBytes() {
		return value/GB;
	}
	
	public long getTBytes() {
		return value/TB;
	}
	
	private void setValue(long value) {
		this.value = value;
	}
}
