package org.apache.uima.ducc.db.portal;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.db.management.DbContext;
import org.apache.uima.ducc.db.management.DbContext.DbMode;
import org.apache.uima.ducc.db.management.exception.MissingDbDriverException;
import org.apache.uima.ducc.db.management.exception.MissingDbHomeException;
import org.apache.uima.ducc.db.management.exception.MissingDbNameException;
import org.apache.uima.ducc.db.management.exception.MissingDbPasswordException;
import org.apache.uima.ducc.db.management.exception.MissingDbProtocolException;
import org.apache.uima.ducc.db.management.exception.MissingDbUserException;

public class DbPortal {
	
	private static final DuccLogger logger = DuccLoggerComponents.getDbLogger(DbPortal.class.getName());
	
	private static DuccId jobid = null;
	
	private DbContext dbContext;
	
	public DbPortal() throws MissingDbHomeException, MissingDbNameException, MissingDbUserException, MissingDbPasswordException, MissingDbDriverException, MissingDbProtocolException {
		dbContext = new DbContext();
		announce();
	}
	
	private void announce() throws MissingDbHomeException, MissingDbNameException, MissingDbUserException, MissingDbPasswordException, MissingDbDriverException, MissingDbProtocolException {
		String location = "announce";
		logger.info(location, jobid, "driver: "+dbContext.getDbDriver());
		logger.info(location, jobid, "home: "+dbContext.getDbHome());
		logger.info(location, jobid, "name: "+dbContext.getDbName());
		//logger.info(location, jobid, "driver: "+dbContext.getDbPassword());
		logger.info(location, jobid, "protocol: "+dbContext.getDbProtocol());
		logger.info(location, jobid, "user: "+dbContext.getDbUser());
	}
	
	public DbContext getDbContext() {
		return dbContext;
	}
	
	public DbMode getDbMode() {
		return dbContext.getDbMode();
	}
	
	public String getDbHome() {
		return dbContext.getDbHome();
	}
	
	public String getDbName() {
		return dbContext.getDbName();
	}
	

}
