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
package org.apache.uima.ducc.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

public class DuccProperties extends Properties {
	public static final String AGENT_LOG_DIR = "ducc.agent.log.dir";
	public static final String AGENT_BROKER_URL = "ducc.agent.broker.url";
	public static final String AGENT_ENDPOINT = "ducc.agent.endpoint";

	private static final long serialVersionUID = 1L;
  
	protected boolean resolvePlaceholders = true;    // Disabled by CLI for JobRequestProperties


	/**
	 * Null constructor now requried because we have a non-null constructor below.
	 */
	public DuccProperties()
	{
		super();
	}
	
    /**
     * Convert a run-of-the-mill properties object into a handsome DuccProperties
     */
    public DuccProperties(Properties p)
    {
        for ( Object k : p.keySet() ) {
            put(k, p.get(k));
        }
    }

	public void load() throws Exception {
		Properties tmp = Utils.loadPropertiesFromClasspathForResource("agent");
		for (Iterator<Entry<Object, Object>> it = tmp.entrySet().iterator(); it
				.hasNext();) {
			Entry<Object, Object> entry = (Entry<Object, Object>) it.next();
			super.put(entry.getKey(), entry.getValue());
		}
	}


    /**
     * Trim comments from the line.
     */
    private String trimComments(String val)
    {
        String answer = "";

        
        int ndx = val.indexOf("#");
        if ( ndx >= 0 ) {
            answer = val.substring(0, ndx);
        } else {
            answer = val.trim();
        }
        
        return answer.trim();
    }

    /**
     * Get the property, trim junk off the end, and try to convert to int.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public int getIntProperty(String k)
    {
        String v = getProperty(k);
        if ( v == null ) {
            throw new MissingPropertyException("Can't find property \"" + k + "\"");
        }
        v = trimComments(v);
        return Integer.parseInt(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to int. If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public int getIntProperty(String k, int dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return Integer.parseInt(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to int.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public long getLongProperty(String k)
    {
        String v = getProperty(k);
        if ( v == null ) {
            throw new MissingPropertyException("Can't find property \"" + k + "\"");
        }
        v = trimComments(v);
        return Long.parseLong(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to int. If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public long getLongProperty(String k, long dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return Long.parseLong(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to double.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public double getDoubleProperty(String k)
    {
        String v = getProperty(k);
        if ( v == null ) {
            throw new MissingPropertyException("Can't find property \"" + k + "\"");
        }
        v = trimComments(v);
        return Double.parseDouble(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to double.  If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public double getDoubleProperty(String k, double dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return Double.parseDouble(v);
    }

    /**
     * Get the property, trim junk off the end, and try to convert to double. If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     * @throws NumberFormattingException if the property cannot be converted to a number.
     */
    public double getLongProperty(String k, double dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return Double.parseDouble(v);
    }

    /**
     * Get the property, trim junk off the end and return it.  If you want the junk, just use getProperty().
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     */
    public String getStringProperty(String k)
    {
        String v = getProperty(k);
        if ( v == null ) {
            throw new MissingPropertyException("Can't find property \"" + k + "\"");
        }
        return trimComments(v);
    }


    /**
     * Get the property, trim junk off the end and return it.  If the default is not
     * found, then return the provided default. If you want the junk, just use getProperty().
     *
     * @param The name of the property to look for.
     *
     * @throws MissingPropertyException if the property does not exist.
     */
    public String getStringProperty(String k, String dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        return trimComments(v);
    }

    public boolean getBooleanProperty(String k, boolean dflt)
    {
        String v = getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        
        v = trimComments(v);
        return ( v.equalsIgnoreCase("t") ||             // sort of cheap - must be t T true TRUE - all else is false
                 v.equalsIgnoreCase("true") );
    }

    public String getProperty(String k)
    {
        String val = super.getProperty(k);
        if ( val != null & resolvePlaceholders && val.contains("${") ) {
            val = Utils.resolvePlaceholders(val, this);
        }
        return val;
    }

	private void override(String configDir) throws Exception {
		String overrideFile = "";

		// Now check if there is an override property file. Using host name
		// check if there is a file <hostname>.properties.
		String hostname = InetAddress.getLocalHost().getHostName();
		if (configDir != null) {
			// Check if there is an override file
			overrideFile = configDir + hostname.concat(".properties");
			File overrides = new File(overrideFile);
			if (overrides.exists()) {
				Properties op = new Properties();
				FileInputStream fis = new FileInputStream(overrideFile);
				op.load(fis);
				fis.close();
				// Override agent properties with those found in the override
				// property file
				for (Entry<Object, Object> value : op.entrySet()) {
					if (super.containsKey((String) value.getKey())) {
						super.remove((String) value.getKey());
					}
					super.put(value.getKey(), value.getValue());
				}
			}
		}

	}

	public void load(String agentPropertyFile) throws Exception {
		String configDir = null;
		//System.out.println("Ducc Component Loading Configuration from Properties File:"
		//		+ agentPropertyFile);
		agentPropertyFile = Utils.resolvePlaceholders(agentPropertyFile);
		FileInputStream fis = new FileInputStream(agentPropertyFile);
		super.load(fis);
		fis.close();
		// Extract a directory where the agent property file lives. Agent will
		// check if there is an override property file that is specific to the
		// node
		configDir = agentPropertyFile.substring(0,
				agentPropertyFile.lastIndexOf(Utils.FileSeparator) + 1);
		override(configDir);
	}

	/**
	 * Disable place-holder resolution when already done and any unresolved entries have been left as-is
	 * for later substitution, e.g. DUCC_SERVICE_INSTANCE, DUCC_OS_ARCH
	 */
  public void ignorePlaceholders() {
    this.resolvePlaceholders = false;
  }
}
