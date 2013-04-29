package org.apache.uima.ducc.test.randomsleep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

public class FixedSleepCR extends CollectionReader_ImplBase {
                
    private volatile Logger logger;
    private volatile ArrayList<Long> workitems;
    private volatile int index = 0;
    private volatile String error_rate = "0";
    private volatile String logdir = "None";
    private volatile String jobid;
    PrintStream jdmark;

    
        public void initialize() throws ResourceInitializationException {       
        super.initialize();

        logger = getLogger();
        logger.log(Level.INFO, "initialize");

        jobid = System.getenv("JobId");
        logdir = jobid + ".output";
        logger.log(Level.INFO, " ****** BB Working directory: " + System.getProperty("user.dir"));
        logger.log(Level.INFO, " ****** BB jobid: " + logdir);

        String jobfile = ((String) getConfigParameterValue("jobfile"));
        logger.log(Level.INFO, " ****** BB jobfile: " + jobfile);

        String comp = ((String) getConfigParameterValue("compression"));
        logger.log(Level.INFO, " ****** BB compression " + comp);

        error_rate = ((String) getConfigParameterValue("error_rate"));
        logger.log(Level.INFO, " ****** BB error_rate " + error_rate);

        Map<String, String> env = System.getenv();
        for ( String k : env.keySet() ) {
            System.out.println(String.format("Environment[%s] = %s", k, env.get(k)));
        }
        File workingdir = new File(System.getProperty("user.dir"));
        File[] files = workingdir.listFiles();
        System.out.println("Working directory is " + workingdir.toString());
        for ( File f : files ) {
            System.out.println("File: " + f.toString());
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

        File f = new File(logdir);
        if ( f.mkdirs() ) {
            String jdmarker = logdir + "/jd.marker";
            try {
                jdmark = new PrintStream(jdmarker);
                jdmark.println("" + System.currentTimeMillis() + " " + jobid + " " + jobfile + " " + workitems.size() + " work items.");
                logger.log(Level.INFO, "Created jdmarker file: " + jdmarker);
            } catch (FileNotFoundException e) {
                logger.log(Level.INFO, " !!!!!! Can't open file: " + jdmarker + ". user.dir = ", System.getProperty("user.dir"));
            }
            
        }else {
            logger.log(Level.INFO, " !!!!!! Can't create log directory " + f.toString() );
            logdir = "None";
        }

    }

    static int get_next_counter = 0;    
    
    public synchronized void getNext(CAS cas) throws IOException, CollectionException 
    {
        logger.log(Level.INFO, " ****** getNext[" + index + "]: " + workitems.get(index) + " getNext invocation " + get_next_counter++);
        String parm = "" + workitems.get(index) + " " + (index+1) + " " + workitems.size() + " " + error_rate + " " + logdir;

        if ( jdmark != null ) {
            jdmark.println("" + System.currentTimeMillis() + " " + parm);
        }

        logger.log(Level.INFO, "getNext");
        cas.reset();
        cas.setSofaDataString(parm, "text");
        index++;
        return;
    }

    public void destroy() 
    {
        logger.log(Level.INFO, "destroy");
        if ( jdmark != null ) {
            jdmark.println("" + System.currentTimeMillis() + " " + jobid + " JD is destroyed");
            jdmark.close();
        }
    }

    
    public void close() throws IOException 
    {
        logger.log(Level.INFO, "close");
        if ( jdmark != null ) {
            jdmark.println("" + System.currentTimeMillis() + " " + jobid + " JD is closed");
            jdmark.close();
        }
        
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
            if ( jdmark == null ) {
                logger.log(Level.INFO, "ALERT jdmarker is null and should not be. Bypassing final closing message.");
            } else {
                jdmark.println("" + System.currentTimeMillis() + " " + jobid + " No more work, hasNext returns " + answer);
                jdmark.close();
                jdmark = null; 
                logger.log(Level.INFO, "getNext() returns false, JDMARKER is closed.");
            }
        }
        return answer;
    }

}
