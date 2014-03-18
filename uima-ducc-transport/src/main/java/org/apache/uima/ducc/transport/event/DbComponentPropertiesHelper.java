package org.apache.uima.ducc.transport.event;

import java.util.Properties;

import org.apache.uima.ducc.transport.event.IDbComponentProperties.ConfigurationStatus;

public class DbComponentPropertiesHelper {

	private Properties properties;
	
	public DbComponentPropertiesHelper(Properties value) {
		properties = value;
	}
	
	public boolean isDisabled() {
		boolean retVal = false;
		if(properties != null) {
			String key = IDbComponentProperties.keyConfigurationStatus;
			String value = properties.getProperty(key);
			if(value != null) {
				switch(ConfigurationStatus.valueOf(value)) {
				case Enabled:
					break;
				case Disabled:
					retVal = true;
					break;
				};
			}
		}
		return retVal;
	}
}
