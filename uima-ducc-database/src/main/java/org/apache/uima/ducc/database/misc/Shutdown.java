package org.apache.uima.ducc.database.misc;


public class Shutdown
{
    public Shutdown()
    	throws Exception
    {
        System.getProperties().put("orientdb.config.file", "/home/challngr/ducc_runtime/resources/database.xml");
    }

    public void run()
    	throws Exception
    {
    	//OServerShutdownMain();
    }

    public static void main(String[] args)
        throws Exception 
    {
        try {
            Shutdown s = new Shutdown();
            s.run();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
