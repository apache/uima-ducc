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
package org.apache.uima.ducc.common.persistence.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class StateServices implements IStateServices {
	
	private DuccLogger logger = null;
	
	StateServices() {
	}

    public boolean init(DuccLogger logger)
    {
    	this.logger = logger;
        mkdirs();
        return true;
    }

	private void mkdirs() {
		IOHelper.mkdirs(svc_reg_dir);
		IOHelper.mkdirs(svc_hist_dir);
	}

    private String mkfilename(long id, String type)
    {
        return svc_reg_dir + Long.toString(id) + "." + type;
    }

    private String mkfilename(DuccId id, String type)
    {
        return mkfilename(id.getFriendly(), type);
    }

	private ArrayList<Long> getList(String type) {
		
		String location = "getList";
		ArrayList<Long> retVal = new ArrayList<Long>();
		try {
			logger.debug(location, null, svc_reg_dir);
			File folder = new File(svc_reg_dir);
			File[] listOfFiles = folder.listFiles();
			if(listOfFiles != null) {
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						String name = listOfFiles[i].getName();
						if(name.endsWith("."+type)) {
                            int ndx = name.lastIndexOf(".");
                            name = name.substring(0, ndx);
                            retVal.add(Long.parseLong(name));
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, null, e);
		}
		return retVal;
	}

	
	public ArrayList<Long> getSvcList() {
		return getList(IStateServices.svc);
	}

	
	public ArrayList<Long> getMetaList() {
		return getList(IStateServices.meta);
	}
	
	private DuccProperties getProperties(String name) {
		String location = "getProperties";
		DuccProperties properties = new DuccProperties();
		try {
			FileInputStream fis = new FileInputStream(name);
			try {
				properties.load(fis);
			}
			finally {	
				fis.close();
			}
		}
		catch(Exception e) {	
			logger.error(location, null, e);
		}
		return properties;
	}
	
	
	public StateServicesDirectory getStateServicesDirectory() 
        throws Exception 
    {
		String location = "getStateServicesDirectory";
		StateServicesDirectory ssd = null;
		try {
			ssd = new StateServicesDirectory();
			ArrayList<Long> svcList = getSvcList();
			logger.trace(location, null, svcList.size());
			for(Long entry : svcList) {
				try {
                    StateServicesSet sss = new StateServicesSet();
                    String fnSvc = mkfilename(entry, svc);
                    String fnMeta = mkfilename(entry, meta);
                    DuccProperties propertiesSvc = getProperties(fnSvc);
                    sss.put(svc, propertiesSvc);
                    DuccProperties propertiesMeta = getProperties(fnMeta);
                    sss.put(meta, propertiesMeta);
                    ssd.put(entry, sss);
				}
				catch(Exception e) {
					logger.error(location, null, e);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, null, e);
		}
		return ssd;
	}

    // Try to write properties file, using a temp file as backup in case it fails.
    private boolean writeProperties(DuccId id, Properties props, File pfile, File pfile_tmp, String type)
    {
    	
    	String methodName = "saveProperties";
        FileOutputStream fos = null;

        long original_size = pfile.length();
        try {
            if ( (!pfile.exists()) || pfile.renameTo(pfile_tmp) ) {
                fos = new FileOutputStream(pfile);
                props.store(fos, type + " Descriptor");
            } else {
                logger.error(methodName, id, "Cannot save", type, "properties, rename of", pfile, "to", pfile_tmp, "fails.");
                if ( (!pfile.exists()) && pfile_tmp.exists() ) {
                    if ( !pfile_tmp.renameTo(pfile) ) {
                        logger.error(methodName, id, "Cannot restore", pfile_tmp, "to", pfile, "after failed update.");
                    }
                }
            }
		} catch (FileNotFoundException e) {
            logger.error(methodName, id, "Cannot save " + type + " properties, file does not exist.");
		} catch (IOException e) {
            logger.error(methodName, id, "I/O Error saving " + type + " service properties: " + e.toString());
		} catch (Throwable t) {
            logger.error(methodName, id, "Unexpected Error saving " + type + " service properties: " + t.toString());
		} finally {
            try {
				if ( fos != null ) fos.close();
                long updated_size = pfile.length();
                long tmp_size = pfile_tmp.length();

                // updated size must be > 0 and tmp_size must match original size
                if ( (updated_size > 0) && (original_size == tmp_size) ) {
                    pfile_tmp.delete();
                } else {
                    logger.error(methodName, id, "Update of", pfile.toString(), "failed.  Original size:", original_size, "updated size", updated_size, "temp file size", tmp_size);
                    logger.error(methodName, id, "The updated size must be > 0 and the temp size must match the original size for sucess.");
                    logger.error(methodName, id, "Attempting to restore", pfile.toString(), "from", pfile_tmp.toString());
                    if ( !pfile.exists() && pfile_tmp.exists() ) {
                        pfile_tmp.renameTo(pfile);
                    }
                    return false;
                }
			} catch (Throwable t) {
                logger.error(methodName, id, "Cannot close", type, "properties:", t);
                return false;
			}
        }

        return true;
    }

    // Try, and retry, to save a single props file.  Returns true if ok, false otherwise.
    private boolean saveProperties(DuccId id, Properties props, File pfile, File pfile_tmp, String type)
    {    	
        int max = 5;
        for ( int i = 0; i < max; i++ ) {
            if (writeProperties(id, props, pfile, pfile_tmp, type)) return true;
        }
        return false;
    }


    // Attempt to transactionally save the two service props. If it returns null
    // the caller can assume it worked.  Otherwise and error string indicating the cause
    // of failure is returned.  In case of failure, we try to insure no properties 
    // file linger.
    public boolean storeProperties (DuccId id, Properties svcprops, Properties metaprops)
    {
        // save svc and meta in a transaction

        File    svcfile   = new File(mkfilename(id, svc));
        File    metafile  = new File(mkfilename(id, meta));
        boolean ok        = true;

        File   tmpfile = new File(svcfile.toString() + ".tmp");
        if ( saveProperties(id, svcprops, svcfile, tmpfile, svc) ) {
            tmpfile = new File(metafile.toString() + ".tmp");
            ok = saveProperties(id, metaprops, metafile, tmpfile, meta);
        } 

        if ( !ok ) {
            metafile.delete();            
            svcfile.delete();
        }
        return ok;
    }

    private boolean updateProperties(DuccId serviceId, Properties props, String type)
    {
        File f  = new File(mkfilename(serviceId, type));
        File tmpf = new File(f.toString() + ".tmp");
        return saveProperties(serviceId, props, f, tmpf, type);
    }

    public boolean updateJobProperties(DuccId serviceId, Properties props)
    {
        return updateProperties(serviceId, props, svc);
    }

    public boolean updateMetaProperties(DuccId serviceId, Properties props)
    {
        return updateProperties(serviceId, props, meta);
    }

    public void deleteProperties(long serviceId)
    {
        String  svcfile   = mkfilename(serviceId, svc);
        String  metafile  = mkfilename(serviceId, meta);

        File pf = new File(svcfile);
        pf.delete();
        
        File mf = new File(metafile);
        mf.delete();           
    }

    public void deleteProperties(DuccId id)
    {
        deleteProperties(id.getFriendly());
    }


    public void moveToHistory(DuccId id, Properties svc, Properties meta)
        throws Exception
    {
        String methodName = "moveToHistory";

        // Save a copy in history, and then delete the original
        File mfh = new File(svc_hist_dir + id + ".meta");
        try {
            FileOutputStream fos = new FileOutputStream(mfh);
            meta.store(fos, "Archived meta descriptor");            
            fos.close();
        } catch (Exception e) {
            logger.warn(methodName, null, id + ": Unable to save history to \"" + mfh.toString(), ": ", e.toString() + "\"");
        }
        
        String meta_filename = svc_reg_dir + id + ".meta";
        File mf = new File(meta_filename);
        mf.delete();


        File pfh = new File(svc_hist_dir + id + ".svc");

        try {
            FileOutputStream fos = new FileOutputStream(pfh);
            svc.store(fos, "Archived svc properties.");            
            fos.close();
        } catch (Exception e) {
            logger.warn(methodName, null, id + ":Unable to save history to \"" + pfh.toString(), ": ", e.toString() + "\"");
        }

        String props_filename = svc_reg_dir + id + ".svc";
        File pf = new File(props_filename);
        pf.delete();
    }

    public void shutdown() {}
}
