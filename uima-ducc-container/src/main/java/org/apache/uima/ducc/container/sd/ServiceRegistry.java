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

package org.apache.uima.ducc.container.sd;

/**
 * Maintains a registry of services and the details needed to provide access. The format of the
 * details is application specific, e.g. could be simply "host:port"
 *
 */
public interface ServiceRegistry {

  /**
   * Establish connection to the specified registry
   * 
   * @param location
   *          - The location of the registry
   * @return - true if connection succeeds
   */
  boolean initialize(String location);
  
  /**
   * Register an instance of the service along with application-specific details
   *
   * @param name
   *          - The name of the service
   * @param address
   *          - The address/url of the service instance
   * @param details
   *          - Extra details about the instance
   *
   * @return - null or the previous details if this address was already registered
   */
  String register(String name, String address, String details);

  /**
   * Queries all registered instances. Returns an array of instances, each holding a 2-element array
   * holding the address & details
   *
   * @param name
   *          - service name
   * @return - a Nx2 2-D array of addresses & details
   */
  String[][] query(String name);

  /**
   * Fetches the address of a service instance, blocks if none available. 
   * If more than 1 is available chooses which to return using an appropriate algorithm,
   * e.g. least-used or round-robin or random or ...
   *
   * @param name
   *          - service name
   * @return - address
   */
  String fetch(String name);

  /**
   * Remove an entry.
   *
   * @param name
   *          - service name
   * @param address
   *          - Indicates which instance to remove.
   *
   * @return - true if succeeds
   */
  boolean unregister(String name, String address);

}