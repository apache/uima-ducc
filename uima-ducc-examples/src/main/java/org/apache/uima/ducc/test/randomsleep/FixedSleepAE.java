package org.apache.uima.ducc.test.randomsleep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * challngr@us.ibm.com
 * May, 2011
 */

public class FixedSleepAE extends CasAnnotator_ImplBase 
{

    Random r;
    Logger logger;
    static boolean initComplete = false;
    Marker marker;
    String AE_Identifier = "*^^^^^^^^^ AE ";

    ArrayList< long[] > bloated_space = new ArrayList< long[] >();

    @Override
    public void initialize(UimaContext uimaContext) throws ResourceInitializationException 
    {
        super.initialize(uimaContext);

        long tid = Thread.currentThread().getId();

        Map<String, String> env = System.getenv();
        RuntimeMXBean rmxb = ManagementFactory.getRuntimeMXBean();
        String pid = rmxb.getName();
        long seed = System.currentTimeMillis();
        r = new Random(seed);

        logger = UIMAFramework.getLogger(FixedSleepAE.class);
        if ( logger == null ) {
            System.out.println("Is this nuts or what, no logger!");
        }

        
        if ( initComplete ) {
            logger.log(Level.INFO, "Init bypassed in PID:TID " + pid + ":" + tid + ", already completed. ");
            return;
        } else {
            logger.log(Level.INFO, "Init procedes in PID:TIDs " + pid + ":" + tid + " Environment:");
            for ( String k : env.keySet() ) {
                logger.log(Level.INFO, String.format("Environment[%s] = %s", k, env.get(k)));
            }
            File workingdir = new File(System.getProperty("user.dir"));
            File[] files = workingdir.listFiles();
            logger.log(Level.INFO, "Working directory is " + workingdir.toString());
            for ( File f : files ) {
                logger.log(Level.INFO, "File: " + f.toString());
            }
        }

        if ( System.getenv( "FAST_INIT_FAIL" ) != null ) {
            // we want just enough init to get at least one process into RUNNING state, but all
            // subsequent initializations to fail under this scenario.
            String jobid = System.getProperty("ducc.job.id");
            String wd = System.getProperty("user.dir");
            String markerdir = wd + "/" + jobid + ".output";
            System.out.println("LOOK IN " + markerdir);
            File marker = new File(markerdir);
            
            String[] outputs = marker.list();
            int count = 0;
            for ( String s : outputs) {
                if ( s.endsWith(".die") ) count++;
                System.out.println("COUNT " + s + " count = " + count);
                if ( count > 2 ) {
                    throw new IllegalStateException("foo foo and foo");
                }
            }
            File init_death = new File(markerdir + "/" + tid + ".die");
            System.out.println("CREATE " + init_death);
            try {
                init_death.createNewFile();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }


        long sleep;
        if ( !initComplete ) {                                    // longer init only the first tim
            initComplete = true;
        } 

        String itime = System.getenv("AE_INIT_TIME");        // the minimum time to sleep
        String irange = System.getenv("AE_INIT_RANGE");      // the range of a random amount added to the minimum time
        String ierror = System.getenv("INIT_ERROR");         // probability of init error, int, 0:100

        if ( itime == null ) {
            throw new IllegalArgumentException("Missing AE_INIT_TIME");
        }

        if ( irange == null ) {
            throw new IllegalArgumentException("Missing AE_INIT_RANGE");
        }

        int i_itime;
        int i_irange;
        try {
            i_itime = Integer.parseInt(itime);
            i_irange = Integer.parseInt(irange);
        } catch (NumberFormatException e) {
            logger.log(Level.INFO, "Invalid AE_INIT_TIME[" + itime + "] or AE_INIT_RANGE[" + irange + "] - must be numbers.");
            throw e;
        }

        if ( i_itime < 0 ) {
            throw new IllegalArgumentException("Invalid AE_INIT_TIME, must be >= 0");
        }

        if ( i_irange <= 0 ) {
            throw new IllegalArgumentException("Invalid AE_INIT_RANGE, must be > 0");
        }

        if ( ierror != null ) {
            int probability = Integer.parseInt(ierror);
            int toss = nextrand(100);
            logger.log(Level.INFO, "Init errors: probability[" + probability + "] toss[" + toss + "]");
            if ( probability > toss ) {
                throwAnException("Random Error in Initialization");
            }
        }
        
        sleep = i_itime + nextrand(i_irange);  // pick off some random number of milliseconds, min of 5 minutes init sleep

        logger.log(Level.INFO, "^^--------> Initialization sleep time is " + sleep + " milliseconds");
                   
        String bloat = System.getenv("INIT_BLOAT");
        if ( bloat != null ) {
            logger.log(Level.INFO, "INIT_BLOAT is set to " + bloat + "; starting bloat in init");
            runBloater(bloat);
        }
        
        String ok = "INTERRUPTED";
        logger.log(Level.INFO, "^^-------> AE process " + pid + " TID " + tid + " initialization starts: sleep " + sleep + "MS");
        try {
            Thread.sleep(sleep);
            ok = "OK";
        } catch (InterruptedException e) {
            logger.log(Level.INFO, "^^-------> AE process " + pid + " TID " + tid + " my sleep has been disturbed!");
        }
        logger.log(Level.INFO, "^^-------> AE process " + pid + " TID " + tid + " initialization " + ok);
        return;
    }
        
    /**
     * Need to simulate a process that leaks.  We just allocate stuff until we die somehow.  
     * Careful, this can be pretty nasty if not contained by the infrastructure.  
     *
     * Older code = use the Bloater class for better results.
     */
    void runBloater(String gb)
    {
        HashMap<Object, Object> bloat = new HashMap<Object, Object>();
        int ndx = 0;
        long total = 0L;
        long limit = Long.parseLong(gb);
        limit *= (1024 * 1024 * 1024);
        while (true) {
            long[] waste = new long[4096];
            for ( int i = 0; i < waste.length; i++ ) {
                waste[i] = i;
            }

            bloat.put(new Integer(ndx++), waste);
            total += (waste.length * 8L);

            if ( ndx % 1000 == 0 ) {       // only print every 1000th iteration or so
                System.out.println("Total " + total + " limit " + limit);
            }
            if ( total > limit ) { // we stop when < 2G left
                System.out.println("Stopping allocation at " + ("" + (total / ( 1024*1024))) + " just hanging around now.");
                while (true) {
                    try {
                        Thread.sleep(10000);
                    } catch ( Throwable t ) {
                        return;
                    }
                }
            }
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
            arguments[0] = "Fake AEPE Argument number " + i;
        }

        //
        // These first are the checked UIMA exceptions that we have to catch
        //
    
        try {
            switch ( whichmessage ) {
            case 2:
                throw new AnalysisEngineProcessException();
            case 3:
                throw new AnalysisEngineProcessException("A deliberate error", arguments);
            case 4:
                throw new AnalysisEngineProcessException("A deliberate error with a cause", arguments, new IllegalStateException("Fake ISE"));
            case 5:
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
            throw new IllegalStateException(msgheader + " test message.");
        case 1:
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

    void randomError(double error_rate, String msgheader)
    //throws Exception
    {
        //
        // error_rate is a percentage as a float, e.g. .1 is 1/10th of 1 percent, 20 is 20 percent
        // We'll throw a random [0:9999], or 10,000 possible rands.
        // If the random < 10000 * (rate/100) [ converting rate to 0:1 range] we have an error.
        //
        final int RANGE = 10000;
        if (  error_rate == 0.0 ) {
            dolog(msgheader, "Error rate is 0, bypassing random error");
            return;
        }

        long cointoss = nextrand(RANGE);      // pick off some random number up to 10000
        String msg = msgheader + " simulated error.";        
        
        int check = (int) Math.round(RANGE * (error_rate / 100.0));
        dolog("**-------> AE Error Coin toss " + cointoss + " vs " + check + ": " + (cointoss < check));
        if ( cointoss < check ) {
            throwAnException(msg);
        }
        //throw new AnalysisEngineProcessException(msg);
    }

    void dolog(Object ... args)
    {
        StringBuffer sb = new StringBuffer();
        for ( Object s : args ) {
            sb.append(s);
            sb.append(" ");
        }
        String s = sb.toString();
        System.out.println("FROM PRINTLN: " + s);
        logger.log(Level.INFO, "FROM LOGGER:" + s);
        if ( marker != null ) {
            marker.write(s);
        }
    }

    public void destroy()
    {
        System.out.println(AE_Identifier + " Destroy is called (0)");
        dolog("Destroy is called (1) !");        
        try {
            Thread.sleep(3000);                         // simulate actual work being done here
        } catch (InterruptedException e) {
        }
        //      check if process() has been called. If not, marker is still null
        if ( marker != null ) {
            marker.flush();
            marker.close();
        }
        System.out.println(AE_Identifier + " Destroy exits");
    }

    @Override
        public void process(CAS cas) throws AnalysisEngineProcessException {
        String data = cas.getSofaDataString();

        //
        // Parms are in a single 4-token string:
        //   elapsed time in MS for this WI
        //   task id
        //   total tasks
        //   simulated error rate.
        //
        StringTokenizer tok = new StringTokenizer(data);

        long          elapsed    = Long.parseLong(tok.nextToken());
        int           qid        = Integer.parseInt(tok.nextToken());
        int           total      = Integer.parseInt(tok.nextToken());
        double        error_rate = Double.parseDouble(tok.nextToken());
        String        logid      = tok.nextToken();

        RuntimeMXBean rmxb       = ManagementFactory.getRuntimeMXBean();
        String        pid        = rmxb.getName();
        String        completion = "INTERRUPTED";
        long          tid        = Thread.currentThread().getId();
        // boolean       error      = false;

        String        msgheader   = "**-------> AE process " + pid + " TID " + tid + " task " + qid + " of " + total;

        if ( System.getenv( "FAST_INIT_FAIL" ) != null ) {
            // must insure nothing gets done in this case.
            System.out.println("Croakamundo.");
            System.exit(1);
        }

        if ( marker == null) {
            if  (!logid.equals("None") ) {
                marker = new Marker(logid, pid, tid);
            } 
        } else {
            marker.flush();
        }

        try{ 
            dolog(msgheader + " sleeping " + elapsed + " MS.");
            String bloat = System.getenv("PROCESS_BLOAT");
            if ( bloat != null ) {
                long gb = Long.parseLong(bloat) * 1024 * 1024 * 1024;
                Bloat bl = new Bloat(msgheader, gb, elapsed);
                bl.start();
            }

            randomError(error_rate, msgheader);           
            Thread.sleep(elapsed);
            completion = "OK";
            dolog(msgheader + " returns after " + elapsed + " MS completion " + completion);
        } catch (InterruptedException e) {
            dolog(msgheader + " my sleep has been rudely interrupted!");
            // error = true;
        } 
        //         catch ( Throwable t ) {
        //             dolog(msgheader + " Unexpected exception: " + t.getMessage());
        //             error = true;
        //         } finally {
        //             dolog(msgheader + " returns with error: " + error);
        //         }

    }

    class Marker
    {
        PrintWriter writer = null;

        ArrayList<String> lines = new ArrayList<String>();
        
        Marker(String filestem, String pid, long tid)
        {
            String filename = filestem + "/AE." + pid + "." + tid + ".marker";
            try {
                writer = new PrintWriter(filename);
                writer.println(now() + " AE starts marker. Pid["+ pid + "] + tid[" + tid + "]");
            } catch (FileNotFoundException e) {
                System.out.println(" !!!!!! Can't open file: " + filename + ". user.dir = " + System.getProperty("user.dir"));
                writer = null;
            }        
        }

        String now()
        {
            return "" + System.currentTimeMillis();
        }

        void write(String line)
        {
            if ( writer != null ) {
                lines.add(line);
            }
        }                

        void flush()
        {
            if ( writer != null ) {
                writer.println("------------------------------------------------------------------------------------------");
                for ( String s : lines ) {
                    writer.println(now() + " " + s);
                }
                writer.println("------------------------------------------------------------------------------------------");
                writer.flush();
                lines.clear();
            }
        }

        void close()
        {
            if ( writer != null ) {
                flush();
                writer.close();
            }
        }
    }

    class Bloat
        extends Thread
    {
        int NUM_UPDATES = 10;
        long howmuch;
        long elapsed;
        String msgheader;
        //
        // want to bloat to max about halfway before the sleep exits, if possible
        //
        Bloat(String msgheader, long howmuch, long elapsed)
        {
            this.msgheader = msgheader;
            this.howmuch = howmuch;            // total bloat, in bytes
            this.elapsed = elapsed;            // how long this process will live
        }
        
        void increase()
        {
            long amount = howmuch / NUM_UPDATES;
            long current = 0;
            long increment = 1024*1024*1024/8;                 // a gigish, in longs
            while ( current < amount ) {                
            	dolog(msgheader + " ====> Allocating " + (increment*8) + " bytes.");
                long[]  longs = new long[ (int) increment ];  // approximately howmuch/NUM_UPDATES bytes
                bloated_space.add(longs);
                current += (increment*8);
            	dolog(msgheader + " ====> Current " + current );
            }
            dolog(msgheader + " ====> Allocated " + current + " bytes.");
        }
        
        public void run()
        {
            long bloat_target = elapsed/2;              // want to fully bloat after this long
            long sleep_time = bloat_target/NUM_UPDATES; // will do in NUM_UPDATES increments, sleep this long
            long total = 0;                             // how long i've slept
            dolog(msgheader + " Starting bloater: " + howmuch + " bytes over " + bloat_target + " ms.");
            while (total < bloat_target ) {             // done yet?
                increase();                             // bloat a bit
                try {
                    dolog(msgheader + " Sleeping " + sleep_time + "ms");
                    Thread.sleep(sleep_time);
				} catch (InterruptedException e) {
					// don't care
				} 
                total += sleep_time;                   // account for it
            }
        }
    }
}
