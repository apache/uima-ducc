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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * Loads a configuration properties file from the classpath used to
 * connect pull services to their clients via a registry.
 * e.g. the Incremental Ingestion services:
 *    -  TAS, Note & Summary need to find the DB
 *    -  Note & Summary need to find TAS
 *    -  DB and TAS must register their locations.
 *
 * Uses the environment or system properties to replaces ${variable} placeholders
 */
public class ConfigurationProperties {

  private static final String service_configuration_property = "ducc.service.configuration";
  private static ConfigurationProperties instance = null;
  private Properties props;

  synchronized static public Properties getProperties() {
    if (instance == null) {
      instance = new ConfigurationProperties();
    }
    return instance.props;
  }

  private ConfigurationProperties() {
    // The properties file may have an optional suffix of the form "?key=value"
    // allowing an override of one of the variables in the file
    String propertyFile = System.getProperty(service_configuration_property);
    if (propertyFile == null) {
      throw new IllegalArgumentException("Missing value for system property: " + service_configuration_property);
    }
    String pVal = null, pKey = null;
    String[] fileParts = propertyFile.split("\\?");
    propertyFile = fileParts[0];
    if (fileParts.length > 1) {
        String[] paramParts = fileParts[1].split("=");
        pKey = paramParts[0];
        pVal = paramParts[1];
    }
    InputStream inputStream = ConfigurationProperties.class.getClassLoader().getResourceAsStream(propertyFile);
    if (inputStream != null) {
      props = new Properties();
      try {
        props.load(inputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      for ( Entry<Object, Object> entry : props.entrySet()) {
        String value = (String) entry.getValue();
        // Replace placeholders of the form ${var-name} with a value from
        // the ? override, the environment, or the system properties
        int i = value.indexOf("${");
        if (i >= 0) {
          do {
            int j = value.indexOf('}', i);
            if (j > 0) {
              String key = value.substring(i+2, j);
              String val;
              if (key.equals(pKey)) {
                  val = pVal;
              } else {
                  val = System.getenv(key);
                  if (val == null) {
                      val = System.getProperty(key);
                  }
              }
              value = value.substring(0,i) + val + value.substring(j+1);
            }
            i = value.indexOf("${", i+2);   // Check if more to replace
          } while(i >= 0);
          props.put(entry.getKey(), value);    // Update with expanded value
        }
      }
    } else {
      throw new RuntimeException("Failed to find " + propertyFile + " in the classpath");
    }
  }
}
