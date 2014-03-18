package org.apache.uima.ducc.db;

import java.util.Properties;

public class DbComponentCommonArea {

	private Properties dbComponentProperties = new Properties();
	
	private static DbComponentCommonArea instance = new DbComponentCommonArea();
	
	public static DbComponentCommonArea getInstance() {
		return instance;
	}
	
	public Properties getDbComponentPropertiesCopy() {
		Properties retVal = new Properties();
		retVal.putAll(dbComponentProperties);
		return retVal;
	}
	
	public Properties getDbComponentProperties() {
		return dbComponentProperties;
	}
	
	public String getDbComponentProperty(String key) {
		return dbComponentProperties.getProperty(key);
	}
	
	public void setDbComponentProperty(String key, String value) {
		dbComponentProperties.setProperty(key, value);
	}
	
}
