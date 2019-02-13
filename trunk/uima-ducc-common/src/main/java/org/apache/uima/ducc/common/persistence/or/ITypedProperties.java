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
package org.apache.uima.ducc.common.persistence.or;

import java.util.Map;
import java.util.Properties;

public interface ITypedProperties {

	public static enum SpecificationType { Job, ManagedReservation, Reservation, Service };
	
	public static enum PropertyType { user, system };
	
	public void add(String type, Object name, Object value);
	public void add(String type, Properties properties);
	public Properties remove(String type);
	public Object remove(String type, Object name);
	public Properties queryType(String type);
	public Map<String,Properties> getMap();
}
