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

import org.springframework.scheduling.SchedulingException;

/**
 * Collection of static mostly RM-specific utilties.
 */
public class SystemPropertyResolver
{
    /**
     * Trim comments from the line.
     */
    private static String trimComments(String val)
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

    private static int toInt(String k, String v)
    {
        try {
            return Integer.parseInt(v);
        } catch ( NumberFormatException n ) {
            throw new IllegalArgumentException("Value for int property " + k + " is not a number: " + v);
        }
    }

    private static long toLong(String k, String v)
    {
        try {
            return Long.parseLong(v);
        } catch ( NumberFormatException n ) {
            throw new IllegalArgumentException("Value for long property " + k + " is not a number: " + v);
        }
    }

    /**
     * Get the property from System props, trim junk off the end, and try to convert to int.
     *
     * @param The name of the property to look for.
     *
     * @throws SchedulingException if the property is not found or cannot be converted to a number.
     */
    public static int getIntProperty(String k)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            throw new IllegalArgumentException("Can't find property " + k);
        }
        v = trimComments(v);
        return toInt(k, v);
    }

    /**
     * Get the property from System props, trim junk off the end, and try to convert to int. If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws SchedulingException if the property cannot be converted to a number.
     */
    public static int getIntProperty(String k, int dflt)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return toInt(k, v);
    }

    /**
     * Get the property from System props, trim junk off the end, and try to convert to int.
     *
     * @param The name of the property to look for.
     *
     * @throws SchedulingException if the property does not exist or is not a number.
     */
    public static long getLongProperty(String k)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            throw new IllegalArgumentException("Can't find property " + k);
        }
        v = trimComments(v);
        return toLong(k, v);
    }

    /**
     * Get the property from System props, trim junk off the end, and try to convert to int. If the property
     * cannot be found, return the default instead.
     *
     * @param The name of the property to look for.
     *
     * @throws SchedulingException if the property is not a number.
     */
    public static long getLongProperty(String k, long dflt)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        v = trimComments(v);
        return toLong(k, v);
    }

    /**
     * Get the property from System props, trim junk off the end and return it.  If you want the
     * junk, just use System.getProperty().
     *
     * @param The name of the property to look for.
     *
     * @throws SchedlingException if the property does not exist.
     */
    public static String getStringProperty(String k)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            throw new IllegalArgumentException("Can't find property " + k);
        }
        return trimComments(v);
    }


    /**
     * Get the property, trim junk off the end and return it.  If the default is not
     * found, then return the provided default.
     *
     * @param k    The name of the property to look for.
     * @param dflt The default value
     * @return     The property value or the default
     */
    public static String getStringProperty(String k, String dflt)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        return trimComments(v);
    }

    public static boolean getBooleanProperty(String k, boolean dflt)
    {
        String v = System.getProperty(k);
        if ( v == null ) {
            return dflt;
        }
        
        v = trimComments(v);
        return ( v.equalsIgnoreCase("t") ||             // sort of cheap - must be t T true TRUE - all else is false
                 v.equalsIgnoreCase("true") );
    }

}
