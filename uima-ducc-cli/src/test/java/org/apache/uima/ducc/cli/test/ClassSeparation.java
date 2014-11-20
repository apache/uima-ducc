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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccReservationCancel;
import org.apache.uima.ducc.cli.DuccReservationSubmit;

import com.google.gson.Gson;
// Note: this is required for compilation but DO NOT put it into the runtime
//       classpath or the tests will fail.
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 * Test CLI classpath separation
 * 
 * To run this test, compile with the google Gson jar and the poisoned xstream class from the
 * test package.  Then run WITHOUT Gson, but still with the poisoned xstream.
 *
 * The CLI uses both gson and xstream.  The gson test must throw to succeed, because the test tries
 * to load it and gson should be on the other side of the classloader barrier.
 *
 * The reservation should go through becase the poisoned xstream is kept on "this" side of the
 * classloader barrier and only the good one is used.
 */
public class ClassSeparation
    extends ATestDriver
{
    public ClassSeparation()
    {
    }

    public String[] testsToRun()
    {
        return new String[] {
            "VerifyWrongXstream",          // insure poisoned classes are provided by the "user"
            "LoadGson",                    // Insure we can't get at any of the CLI's open source stuff
            "PoisonedCLI",                 // Insure our poisoned classes don't leak into the CLI
        };
    }

    public void testVerifyWrongXstream(String testid)
        throws Exception
    {
        DomDriver dd = null;
        try {
            dd = new DomDriver();
        } catch ( IllegalStateException e ) {
            String msg = e.getMessage();
            if ( msg.equals("I am not the DomDriver.") ) {
                success(testid, "DomDriver is properly bogus.");
            } else {
                fail(testid, "DomDriver may not be bogus, test is invalid.");
            }
        }

        try {
            XStream xs = new XStream(dd);
        } catch ( IllegalStateException e ) {
            String msg = e.getMessage();
            if ( msg.equals("I am not XStream.") ) {
                success(testid, "XStream is properly bogus.");
            } else {
                fail(testid, "XStream may not be bogus, test is invalid.");
            }
        }

    }

    /**
     * Try to load gson - it must fail.  The CLI uses gson, which it will load, but across a
     * classloader barrier so it can't "leak" back into here.
     */
    public void testLoadGson(String testid)
        throws Exception
    {
        try {
            Map<Integer, String> m = new HashMap<Integer, String>();
            for ( int i = 0; i < 20; i++ ) {
                m.put(i, ""+i);
            }
            Gson g = new Gson();
            String gs = g.toJson(m);
            System.out.println(gs);
            fail(testid, "Loaded gson, should have failed.");
        } catch ( NoClassDefFoundError e ) {
        	success("Test succeeded, gson did not leak from CLI into the test code:"  + e.toString());
        }
    }

    /**
     * Try to poison the CLI.  The CLI needs xstream. We'll put a deliberately poisoned xstream into
     * the classpath when running this test.  The CLI should succeed nonetheless.
     */
    public void testPoisonedCLI(String testid)
        throws Exception
    {

        Properties reserve_props = new Properties();
        DuccReservationSubmit reserve;
        String resid = null;

        reserve_props.setProperty("description", "Reserve And Cancel");
        reserve_props.setProperty("instance_memory_size", "4");
        reserve_props.setProperty("number_of_instances", "2");
        reserve_props.setProperty("scheduling_class", "fixed");
        try {
            reserve = new DuccReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                resid = "" + reserve.getDuccId();
                success(testid, "Reservation", resid, "successful, rc =", ""+reserve.getReturnCode(), ":", reserve.getHostsAsString());
                String[] hosts = reserve.getHosts();
                System.out.println("" + hosts.length + " hosts assigned");
                if ( hosts.length > 0 ) {
                    for ( String h : reserve.getHosts() ) {
                        System.out.println("   " + h);
                    }
                }            
            } else {
                fail(testid, "Reservation failed, rc = " + reserve.getReturnCode());
            }
        } catch ( Throwable e ) {
            fail(testid, "Reservation cannot execute.");
            e.printStackTrace();
        }

        if ( resid == null ) {
            fail(testid, "Bypass cancel because reserve failed.");
            return;
        }

        Properties cancel_props = new Properties();
        cancel_props.setProperty("id", resid);
        try {
            DuccReservationCancel cancel = new DuccReservationCancel(cancel_props);
            if ( cancel.execute() ) {
                success(testid, "Reservation " + ""+cancel.getDuccId() + " cancelled, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
            } else {                
                fail(testid, "Reservation " + ""+cancel.getDuccId() + " cancel failed, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
            }
        } catch ( Throwable t ) {
            fail(testid, "Cancel reseration cannot execute.");
        }

    }

    public static void main(String[] args)
    {
        ClassSeparation tester = new ClassSeparation();
        tester.runTests();
    }

}
