package org.apache.uima.ducc.database.misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.database.DbConstants;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;
import org.apache.uima.ducc.database.DbHandle;
import org.apache.uima.ducc.database.DbLoader;
import org.apache.uima.ducc.database.DbManager;

import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Toy orientdb loader to load a service registry into OrientDb
 * WARNING: No longer correct.
 */

public class SmLoader
{
    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DB");

    DbManager dbManager;
    String dburl = "remote:localhost/DuccState";

    //String jobHistory = System.getProperty("user.home") + "/ducc_runtime/history/jobs";

    // Must sync these two!
    String serviceState = "/home/ducc/ducc_runtime/state/services";
    DbCategory dbcat = DbCategory.SmReg;

    public SmLoader()
        throws Exception
    {
        try {
            dbManager = new DbManager(dburl, logger);
            dbManager.init();
        } catch ( Exception e ) {
            logger.error("SmLoader", null, "Cannot open the state database:", e);
        }        

    }

    public void closeDb()
    {
    	dbManager.shutdown();
    }

    public void restoreService(long id)
    {
    	String methodName = "restoreService";
       // DbHandle h = null;
       // boolean showJson = false;

        logger.info(methodName, null, "Restoring Service", "Not implemented", id);

    }

    static int storeCount = 0;
	public void saveService(long id, Properties svc_props, Properties meta_props)
        throws IOException 
    {
        String methodName = "saveService";
        long now = System.currentTimeMillis();

   
        DbHandle h = null;
        try {

            String cps = (String) svc_props.remove("classpath");
            //String ping_cps = (String) svc_props.remove("service_ping_classpath");
            h = dbManager.open(); 
            OrientVertex svc  = h.createProperties(DbConstants.DUCCID, id, DbVertex.ServiceReg, dbcat, svc_props);
            OrientVertex meta = h.createProperties(DbConstants.DUCCID, id, DbVertex.ServiceMeta, dbcat, meta_props);
            h.addEdge(svc, meta, DbEdge.ServiceMeta);

            if ( cps != null ) {
                Properties cp_props = new Properties();
                cp_props.put("classpath", cps);
                OrientVertex cp = h.createProperties(DbConstants.DUCCID, id, DbVertex.Classpath, dbcat, cp_props);
                h.addEdge(svc, cp, DbEdge.Classpath);
            }

            h.commit();
            logger.info(methodName, null, id, "---------> Time to save Service:", System.currentTimeMillis() - now);
            storeCount++;
        } catch ( Exception e ) {
            if ( h != null ) h.rollback();
            logger.error(methodName, null, id, "Cannot store service registration:", e);
        } finally {
            if ( h != null ) h.close();
        }
                
	}

    public void run()
    {
        String methodName = "run";
        int max = 0;
        //ArrayList<Long> ids = new ArrayList<Long>();
        int c   = 0;
        File dir = new File(serviceState);
        DbHandle h = null;
		try {
			for ( File f : dir.listFiles() ) {
			    String s = f.toString();
			    if ( s.endsWith(".svc") ) {
			        logger.info(methodName, null, "Loading file", c++, ":", f);
			        //IDuccWorkJob job = null;
			        try {
                        Properties svc_props = new Properties();
                        Properties meta_props = new Properties();

                        String[] dirparts = s.split("/");
                        String[] parts = dirparts[dirparts.length-1].split("\\.");
                        long id = 0;
                        try {
                            id = Long.parseLong(parts[0]);
                        } catch (NumberFormatException ee ) {
                            logger.error(methodName, null, "File", f, "does not appear to be a valid registry file.  Skipping.");
                            continue;
                        }

			            FileInputStream svcin = new FileInputStream(f);
			            FileInputStream metain = new FileInputStream(s.replace("svc", "meta"));

			            svc_props.load(svcin);
			            meta_props.load(metain);
			            svcin.close();
			            metain.close();

			            saveService(id, svc_props, meta_props);
			        } catch(Exception e) {
			            logger.info(methodName, null, e);
			        } finally {
			            // restoreJob(job.getDuccId().getFriendly());
			            if ( (max > 0) && (++c > max) ) break;
			        }
			    }                              
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            if ( h != null ) h.close();
            logger.info(methodName, null, "Stored", storeCount, "objects.");
        }
        // Now read them back
        //for ( long l : ids ) {
        //    restoreJob(l);
        // }
    }


    public static void main(String[] args)
    {
    	SmLoader dbl = null;
        try {
            dbl = new SmLoader();
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } finally {
            if ( dbl != null ) dbl.closeDb();
        }
    }

}
