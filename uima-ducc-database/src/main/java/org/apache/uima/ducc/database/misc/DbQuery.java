package org.apache.uima.ducc.database.misc;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.database.DbHandle;
import org.apache.uima.ducc.database.DbManager;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbQuery
{
    DuccLogger logger = DuccLogger.getLogger(DbQuery.class, "JRC");

    DbManager dbManager;
    String dburl = "remote:localhost/ToyHistory";
    // String dburl = "plocal:/users/challngr/DuccHistory/ToyHistory";

    String jobHistory = System.getProperty("user.home") + "/ducc_runtime/history/jobs";
    // String jobHistory = "/home/ducc/ducc_runtime/history/jobs";

    public DbQuery()
        throws Exception
    {
        try {
            dbManager = new DbManager(dburl, logger);
            dbManager.init();
        } catch ( Exception e ) {
            logger.error("HisstoryManagerDb", null, "Cannot open the history database:", e);
        }        

    }

    public void closeDb()
    {
    	dbManager.shutdown();
    }


    public void run()
    	throws Exception
    {
        String methodName = "run";

        long now = System.currentTimeMillis();
        DbHandle h = dbManager.open();
        Iterable<Vertex> q =  h.select("SELECT ducc_dbid FROM VJOB");
        logger.info(methodName, null, "TIme to execute query:", System.currentTimeMillis() - now);
        for ( Vertex v : q ) {
            ODocument d = ((OrientVertex)v).getRecord();
            Long did = d.field("ducc_dbid");
            logger.info(methodName, null, "ID:", did);
        }
    }


    public static void main(String[] args)
    {
    	DbQuery dbl = null;
        try {
            dbl = new DbQuery();
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } finally {
            if ( dbl != null ) dbl.closeDb();
        }
    }

}
