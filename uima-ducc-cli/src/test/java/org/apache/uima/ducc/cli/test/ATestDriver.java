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
package org.apache.uima.ducc.cli.test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract test driver base for CLI tests
 */
public abstract class ATestDriver
{

    List<String> failReasons = new ArrayList<String>();
    List<String> successReasons = new ArrayList<String>();

    public ATestDriver()
    {
    }

    String join(String testid, Object ... toks)
    {
        StringBuffer sb = new StringBuffer();
        sb.append(testid);
        sb.append(" -");
        for ( Object s : toks ) {
            sb.append(" ");
            sb.append(s.toString());
        }
        return sb.toString();
    }

    /**
     * Update running log, and save result for summary.
     *
     * Failure throws, to allow driver ability to easily abort if needed.  
     *
     */
    void abort(String testid, Object ... reason)
        throws FailedTestException
    {
        failReasons.add(join(testid, reason));
        throw new FailedTestException(join(testid, reason));
    }
    
    /**
     * Update running log, and save result for summary.
     *
     * Don't abort so caller can continue.
     *
     */
    void fail(String testid, Object ... reason)
    {
        failReasons.add(join(testid, reason));
        System.out.println(join(testid, reason));        
    }
    
    /**
     * Create running log and save result for summary.
     */
    void success(String testid, Object ... reason)
    {
        successReasons.add(join(testid, reason));
        System.out.println(join(testid, reason));
    }
        
    //
    // Return the number of tests to run
    //
    public abstract String[] testsToRun();


    /**
     * Here define each of your tests thus:
     * public void runTestN()
     * {
     * }
     * for each test 1 to N.  Implement ntests() to return the number of tests
     * to run, 1 through N.  See TestCommandLine.java for a simple example.
     */ 
    void runTests()
    {
        try {

            String[] testset = testsToRun();
            for ( String t : testset ) {        // having too much fun with reflection
                System.out.println(" -------------- START -------------------- " + t + " ---------- START ------------");

                Method m = getClass().getDeclaredMethod("test" + t, String.class);
                m.invoke(this, t);

                System.out.println(" -------------- END ---------------------- " + t + " ---------- END --------------");
                System.out.println(" ");                
            }
                       
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        System.out.println("Test summary: successes[" + successReasons.size() + "] " + "failures[" + failReasons.size() + "]");
        System.out.println("Tests passed:");
        if ( successReasons.size() > 0 ) {
            for (String s : successReasons) {
                System.out.println("   " + s);
            }
        } else {
            System.out.println("    None.");
        }
        
        System.out.println("Tests failed:");
        if ( failReasons.size() > 0 ) {
            for (String s : failReasons) {
                System.out.println("   " + s);
            }
        } else {
            System.out.println("    None.");
        }

    }

}
