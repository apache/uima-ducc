package org.apache.uima.ducc.test.service;

/**
 * Maintains a registry of services and the addresses than can provide access.
 * The format of the address is determined by the application, e.g. host:port,
 * or perhaps section:host:port if the instance handles a particular subset of the possible requests.
 *
 */
public interface ServiceRegistry {

	/**
	 * Set the name of the service to be managed
	 * @param serviceName
	 */
	void setName(String serviceName);

	/**
	 * Register an instance of the service including the address of where it can be accessed.
	 *
	 * @param serviceAddress - The address used by clients to connect to the instance
	 * @param instanceId - The ID specified by DUCC that can be used by the DUCC pinger to manage it.
	 *
	 * @return - null or the previous instanceId if this address already registered
	 */
	String register(String serviceAddress, String instanceId);

	/**
	 * Queries all registered instances
	 *
	 * @return - an array of registered addresses
	 */
	String[] query();

	/**
	 * Remove an entry.  Could be used by the pinger if the instance has failed.
	 *
	 * @param serviceAddress
	 * @return - the instanceId associated with this address so the pinger can ask DUCC to remove it,
	 *           or null of not registered.
	 */
	String unregister(String serviceAddress);

}
