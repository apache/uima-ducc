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
package org.apache.uima.ducc.common.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.utils.IllegalConfigurationException;

public class NodeConfigurationTest
{
    NodeConfiguration nc;

    // test name, short description, expected rc
    String[] configurations = {
        "test1",  "Basic configuration test"                   , "0",      // pass
        "test2",  "Multiple children"                          , "0",      // pass
        "test3",  "Circular references"                        , "1",      // fail
        "test4",  "Duplicate Nodepools"                        , "1",      // fail
        "test5",  "Missing parent"                             , "1",      // fail
        "test6",  "Toplevel NP, parent is not --default--"     , "0",      // pass
        "test7",  "Class references non-existent NP"           , "1",      // fail
        "test8",  "Two NPs with no node file specified"        , "1",      // fail
    };

    List<String> successes = new ArrayList<String>();
    List<String> failures  = new ArrayList<String>();
    public NodeConfigurationTest()
    {
    }

    String resolve(String test)
    {
        return "src/test/resources/node_configuration_data/" + test;
    }

    int  runConfiguration(String test)
    {        
        System.getProperties().setProperty("DUCC_HOME", resolve(test));
        String nodefile = "ducc.nodes";
        String config   = "ducc.classes";
        NodeConfiguration nc = new NodeConfiguration(config, nodefile, null);

        int rc = 0;
        try {
            nc.readConfiguration();                        // if it doesn't crash it must have worked

            // nc.fullValidation(nodefile);        // this too, gonna throw if there's an issue

            nc.printConfiguration();
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file " + config + " does not exist or cannot be read.");
            rc = 1;
        } catch (IOException e) {
            System.out.println("IOError reading configuration file " + config + ": " + e.toString());
            rc = 1;
        } catch (IllegalConfigurationException e) {
            System.out.println(e.getMessage());
            rc = 1;
        }

        return rc;

    }

    void runTests()
    {
        int i = 0;
        for ( i = 0; i < configurations.length; i++ ) {
            String testid = configurations[i++];
            String desc   = configurations[i++];
            int expected = Integer.parseInt(configurations[i]);
            int rc = 0;
            	
            System.out.println("-------------------------------------- Run Test " + testid + " -----------------------------------------------------");
            if ( (rc = runConfiguration(testid)) == expected) {
                successes.add(testid + ": " + desc + "; expected rc=" + expected + " actual rc=" + rc);
            } else {
                failures .add(testid + ": " + desc + "; expected rc=" + expected + " actual rc=" + rc);
            }
            System.out.println("-------------------------------------- End Test " + testid + " -----------------------------------------------------\n");

        }

        System.out.println("-------------------------------------- Summary -----------------------------------------------------");
        System.out.println("Successes: " + successes.size() + " Failures: " + failures.size());
        System.out.println("-------------------------------------- Successes ---------------------------------------------------");
        for (String s : successes) {
            System.out.println(s);
        }
        System.out.println("-------------------------------------- Failures ----------------------------------------------------");
        for (String s : failures) {
            System.out.println(s);
        }
    }

    public static void main(String[] args)
    {
        NodeConfigurationTest nct = new NodeConfigurationTest();
        nct.runTests();
    }


}
