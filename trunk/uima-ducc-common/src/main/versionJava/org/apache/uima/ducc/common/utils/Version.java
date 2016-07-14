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

/**
 * The source for this class is located in 
 *   src/main/versionJava/org/apache/uima/ducc/common/utils
 *   
 * It is processed at build time to create a java source, by substituting
 * values from the build into some fields.
 *   The Java source is put into target/generated-sources/releaseVersion
 *     in the package org.apache.uima.ducc.common.utils
 *
 */

public class Version
{
    private static final String fullVersion = "${project.version}";
    
    public final static String version()
    {
        return fullVersion;
    }
    
    /*
     * This version number is included on each CLI request and checked by DUCC.
     * Change the value when requests are changed in an incompatible way.
     */
    public final static int cliVersion = 1;

    public static void main(String[] args)
    {
        System.out.println(version());
    }

}
