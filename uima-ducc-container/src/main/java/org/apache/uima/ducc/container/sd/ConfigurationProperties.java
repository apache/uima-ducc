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
 * Replaces placeholders of the form ${environment-variable}
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
    String propertyFile = System.getProperty(service_configuration_property);
    if (propertyFile == null) {
      throw new IllegalArgumentException("Missing value for system property: " + service_configuration_property);
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
        // Replace placeholders of the form ${env-var-name}
        int i = value.indexOf("${");
        if (i >= 0) {
          do {
            int j = value.indexOf('}', i);
            if (j > 0) {
              String envKey = value.substring(i+2, j);
              String envValue = System.getenv(envKey);
              value = value.substring(0,i) + envValue + value.substring(j+1);
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
