package org.apache.uima.ducc.db;

import java.util.Properties;

public class DbComponentCommonArea {

	private DbProperties dbProperties = new DbProperties();
	
	private static DbComponentCommonArea instance = new DbComponentCommonArea();
	
	public static DbComponentCommonArea getInstance() {
		return instance;
	}
	
	public Properties getPropertiesCopy() {
		Properties retVal = new Properties();
		retVal.putAll(dbProperties);
		return retVal;
	}
	
	
}
