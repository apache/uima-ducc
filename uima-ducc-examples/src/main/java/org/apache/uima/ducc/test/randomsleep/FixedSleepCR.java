/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.test.randomsleep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

/**
 * Colleciton reader for the system tests.  This reads a java properties file containing "work items" which are
 * actually just sleep times.  Each getNext passes a new sleep time to one of the waiting FixedSleepAE JPs
 * to simulate real work.
 *
 * The CR accepts these overrides:
 *   - jobfile - this is the name of the properties file with the 'work item' sleep tims
 *   - compression - this is a number used to adjust each sleep time and hence the duration 
 *                   of the test.  The sleep time is divided by this number, so a larger
 *                   compression produces a shorter sleep and a faster run.
 */

public class FixedSleepCR extends CollectionReader_ImplBase 
{

    Random r;
                
    private volatile Logger logger;
    private volatile ArrayList<Long> workitems;
    private volatile int index = 0;
    private volatile String logdir = "None";
    private volatile String jobid;

    double error_rate;
    double exit_rate;
    int    bloat;

    ArrayList< Map<Object, Object> > bloated_space = new ArrayList< Map<Object, Object> >();

    public void initialize() throws ResourceInitializationException 
    {       
        super.initialize();

        logger = getLogger();
        logger.log(Level.INFO, "initialize");

        long seed = System.currentTimeMillis();
        r = new Random(seed);

        jobid = System.getenv("DUCC_JOBID");
        logdir = jobid + ".output";
        logger.log(Level.INFO, " ****** BB Working directory: " + System.getProperty("user.dir"));
        logger.log(Level.INFO, " ****** BB jobid: " + logdir);

        String jobfile = ((String) getConfigParameterValue("jobfile"));
        logger.log(Level.INFO, " ****** BB jobfile: " + jobfile);

        String comp = ((String) getConfigParameterValue("compression"));
        logger.log(Level.INFO, " ****** BB compression " + comp);

        Map<String, String> env = System.getenv();
        for ( String k : env.keySet() ) {
            System.out.println(String.format("Environment[%s] = %s", k, env.get(k)));
        }
        File workingdir = new File(System.getProperty("user.dir"));
        File[] files = workingdir.listFiles();
        System.out.println("Working directory is " + workingdir.toString());
        if ( files != null ) {
            for ( File f : files ) {
                System.out.println("File: " + f.toString());
            }
        }

        // set these up for use in getNext
        error_rate = getDoubleFromEnv("CR_RUNTIME_ERROR", false);   // probability of an error in getNext
        exit_rate  = getDoubleFromEnv("CR_RUNTIME_EXIT", false);    // probability of process exit in get Next 
        bloat      = getIntFromEnv   ("CR_GETNEXT_BLOAT", false);   // if set, amount each getNext will bloat the CR, in MB
        bloat      = bloat * 1024 * 1024;                           // to MB

        int i_error  = getIntFromEnv("CR_INIT_ERROR", false);       // probability of init error, int, 0:100
        int i_exit   = getIntFromEnv("CR_INIT_EXIT" , false);

        if ( i_error > 0 ) {
            int toss = nextrand(100);
            logger.log(Level.INFO, "Init errors: probability[" + i_error + "] toss[" + toss + "]");
            if ( i_error > toss ) {
                throwAnException("Random Error in Initialization");
            }
        }

        if ( i_exit > 0 ) {
            int toss = nextrand(100);
            logger.log(Level.INFO, "Init hard exit: probability[" + i_exit + "] toss[" + toss + "]");
            if ( i_exit > toss ) {
                logger.log(Level.INFO, "Init hard exit: croaking hard now.");
                Runtime.getRuntime().halt(19);
            }
        }

        long compression = Long.parseLong(comp);
        workitems = new ArrayList<Long>();

        String times = "5000";
        try {
            FileReader fr = new FileReader(jobfile);
            Properties props = new Properties();
            props.load(fr);
            times = props.getProperty("elapsed");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        StringTokenizer st = new StringTokenizer(times);
        int ndx = 0;
        while ( st.hasMoreTokens() ) {
            long elapsed = Long.parseLong(st.nextToken());
            long compressed = 0;
            if ( compression > 0 ) {
                compressed = elapsed / compression;
            }
            workitems.add(compressed);
            logger.log(Level.INFO, " ****** Adding work item of duration " + elapsed + " ms compressed to " + compressed + " ms as work item " + ndx++);
        }

    }

    int getIntFromEnv(String key, boolean fail)
    {
        logger.log(Level.INFO, "getIntFromEnv: key = " + key + " fail = " + fail);
        String s = System.getenv(key);
        logger.log(Level.INFO, "getIntFromEnv: key = " + key + " fail = " + fail + " s = " + s);
        if ( s == null ) {
            if ( fail ) throw new IllegalArgumentException("Missing " + key);
            else        return 0;
        }

        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            logger.log(Level.INFO, "Invalid " + key + "[" + s + "].  Must be integer.");
            throw e;
        }
    }

    double getDoubleFromEnv(String key, boolean fail)
    {
        String s = System.getenv(key);
        if ( s == null ) {
            if ( fail ) throw new IllegalArgumentException("Missing " + key);
            else        return 0.0;
        }

        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            logger.log(Level.INFO, "Invalid " + key + "[" + s + "].  Must be double.");
            throw e;
        }
    }

    /**
     * This thows all kinds of stuff.
     */
    @SuppressWarnings("null")
    void throwAnException(String msgheader)
    {
        int MAX_EXCEPTIONS = 7;        // deliberately wrong, this is a foul-up simulator after all!

        int whichmessage = nextrand(MAX_EXCEPTIONS);

        Object[] arguments = new Object[3];
        for ( int i = 0; i < 5; i++ ) {
            arguments[0] = "Fake CR Argument number " + i;
        }

        //
        // These first are the checked UIMA exceptions that we have to catch
        //
    
        try {
            switch ( whichmessage ) {
            case 2:
                logger.log(Level.INFO, "FixedSleepCR: Throwing Analysis AnalysisEngineProcessException with no argument."); 
                throw new AnalysisEngineProcessException();
            case 3:
                logger.log(Level.INFO, "FixedSleepCR: Throwing Analysis AnalysisEngineProcessException with string argument."); 
                throw new AnalysisEngineProcessException("A deliberate error", arguments);
            case 4:
                logger.log(Level.INFO, "FixedSleepCR: Throwing Analysis AnalysisEngineProcessException with a string and a wrapped exception."); 
                throw new AnalysisEngineProcessException("A deliberate error with a cause", arguments, new IllegalStateException("Fake ISE"));
            case 5:
                logger.log(Level.INFO, "FixedSleepCR: Throwing Analysis AnalysisEngineProcessException with a wrapped exception."); 
                throw new AnalysisEngineProcessException(new IllegalStateException("Fake ISE"));
            }
        } catch ( Exception e ) {
            throw new RuntimeException(e);
        }

        //
        // These are unchecked exception which we want to throw "raw"
        //
        switch ( whichmessage ) {
            case 0:
                logger.log(Level.INFO, "FixedSleepCR: Throwing IllegalStateExeption with a string.");
                throw new IllegalStateException(msgheader + " test message.");
            case 1:
                logger.log(Level.INFO, "FixedSleepCR: Throwing a NullPointer.");

                Thread notAThread = null;
                @SuppressWarnings("unused")
                long xx = notAThread.getId();               // down with a null pointer!
            break;
            default:
                throw new IllegalStateException(msgheader + " -- Message " + whichmessage + " seems to blow the case statment in the test!");
        }

    }

    int nextrand(int max)
    {
        return ( ((int) r.nextLong()) & Integer.MAX_VALUE) % max;
    }

    void randomError(double error_rate, String msgheader, boolean do_exit)
    //throws Exception
    {
        //
        // error_rate is a percentage as a float, e.g. .1 is 1/10th of 1 percent, 20 is 20 percent
        // We'll throw a random [0:9999], or 10,000 possible rands.
        // If the random < 10000 * (rate/100) [ converting rate to 0:1 range] we have an error.
        //
        final int RANGE = 10000;
        if (  error_rate == 0.0 ) {
            logger.log(Level.INFO, msgheader + " Error rate is 0, bypassing random error");
            return;
        }

        long cointoss = nextrand(RANGE);      // pick off some random number up to 10000
        String msg = msgheader + " simulated error.";        
        
        int check = (int) Math.round(RANGE * (error_rate / 100.0));
        logger.log(Level.INFO, "**-------> CR Error Coin toss " + cointoss + " vs " + check + ": " + (cointoss < check) + (do_exit ? " Exiting." : " Throwing."));
        if ( cointoss < check ) {
            if ( do_exit ) {
                Runtime.getRuntime().halt(19);
            } else {
                throwAnException(msg);
            }
        }
        //throw new AnalysisEngineProcessException(msg);
    }

    /**
     * Need to simulate a process that leaks.  We just allocate stuff until we die somehow.  
     * Careful, this can be pretty nasty if not contained by the infrastructure.  
     *
     * Older code = use the Bloater class for better results.
     */
    void runBloater(int next_bloat, String msgheader)
    {
        if ( next_bloat <= 0 ) return;

        Map<Object, Object> bloated_map = new HashMap<Object, Object>();
        int ndx = 0;

        int current_bloat = 0;

        while ( current_bloat < next_bloat ) {
            long[] waste = new long[4096];                   // a full 4096 * 8 bytes +/- a few
            for ( int i = 0; i < waste.length; i++ ) {       // touch them or they may never be actually allocated
                waste[i] = i;
            }

            bloated_map.put(new Integer(ndx++), waste);      // into our map
            current_bloat += (waste.length * 8L);            // account for space until done
        }
        bloated_space.add(bloated_map);                      // don't lose the reference, to insure we do bloat
        logger.log(Level.INFO, msgheader + " Blated by " + (current_bloat) + " more bytes");
    }

    static int get_next_counter = 0;    
    
    public synchronized void getNext(CAS cas) throws IOException, CollectionException 
    {

        String msgheader = " ****** getNext[" + index + "]: ";
        logger.log(Level.INFO, msgheader + workitems.get(index) + " getNext invocation " + get_next_counter++);
        String parm = "" + workitems.get(index) + " " + (index+1) + " " + workitems.size() + " " + logdir;

        randomError(error_rate, msgheader, false);           
        randomError(exit_rate, msgheader, false);
        runBloater(bloat, msgheader);

        logger.log(Level.INFO, "getNext");
        cas.reset();
        cas.setSofaDataString(parm, "text");
        index++;
        return;
    }

    public void destroy() 
    {
        logger.log(Level.INFO, "destroy");
    }

    
    public void close() throws IOException 
    {
        logger.log(Level.INFO, "close");        
    }

    
    public Progress[] getProgress() 
    {
        logger.log(Level.INFO, "getProgress");
        ProgressImpl[] retVal = new ProgressImpl[1];
        retVal[0] = new ProgressImpl(index,workitems.size(),"WorkItems");
        return retVal;
    }

    
    public boolean hasNext() throws IOException, CollectionException 
    {
        logger.log(Level.INFO, "hasNext");
        boolean answer = (index < workitems.size());
        if ( ! answer ) {
            logger.log(Level.INFO, "" + System.currentTimeMillis() + " " + jobid + " No more work, hasNext returns " + answer);
        }
        return answer;
    }

}
