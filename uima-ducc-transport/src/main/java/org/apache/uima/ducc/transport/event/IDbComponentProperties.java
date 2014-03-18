package org.apache.uima.ducc.transport.event;

public interface IDbComponentProperties {

	public enum ConfigurationStatus { Enabled, Disabled };
	
	public String keyConfigurationStatus = ConfigurationStatus.class.getName();
	
	public enum Keys { 
		keyConfigruationStatus
	};
}
