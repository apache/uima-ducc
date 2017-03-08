package org.apache.uima.ducc.test.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a simple service registry using the shared filesystem.
 * Create a folder named for for each service in a shared base registry directory.
 * In this folder create a file named for each service-address containing the instance-id.
 * Encode/decode the service name and address into valid filenames.
 *
 * Entries can only be be removed by their creator.
 * Entries should have the same visibility as the base registry directory.
 * >>> Could let the creator control the visibility
 *
 */
public class FileServiceRegistry implements ServiceRegistry {

	String serviceName;
	Map<String,Map<String,String>> serviceNameMap = new HashMap<String,Map<String,String>>();
	Map<String,String> serviceMap;
	private File registryDir;
	private File serviceDir;

	static public ServiceRegistry getInstance(String registryUrl) {
	    if (registryUrl.startsWith("file://")) {
	        return new FileServiceRegistry(registryUrl.substring(7));
	    } else {
	        System.err.println("EROR: Unsopprted registry URL: " + registryUrl);
	        return null;
	    }
	}

	private FileServiceRegistry(String registerDirectory) {
		registryDir = new File(registerDirectory);
	}

	@Override
	public void setName(String serviceName) {
		this.serviceName = serviceName;
		serviceDir = new File(registryDir, encode(serviceName));
		if (!registryDir.exists()) {
			registryDir.mkdir();
		}
	}

	// Create file "serviceAddress" holding "instanceId"
	// Return previous value if already registered, or null if not.
	@Override
	public String register(String serviceAddress, String instanceId) {
		String prevId = null;
		if (!serviceDir.exists()) {
			serviceDir.mkdir();
		}
		// Could be a race condition if the dir is deleted at this time
		File instanceFile = new File(serviceDir, encode(serviceAddress));
		if (instanceFile.exists()) {
			try (BufferedReader reader = new BufferedReader(new FileReader(instanceFile))) {
				prevId = reader.readLine();
			} catch (IOException e) {
			    System.err.println("ERROR: Failed to read instanceId when updating registry file: " + instanceFile.getAbsolutePath() + " - " + e);
			}
		}
		try (PrintWriter writer = new PrintWriter(instanceFile)) {
			writer.println(instanceId);
		} catch (FileNotFoundException e) {
		    System.err.println("ERROR: Failed to create registry file: " + instanceFile.getAbsolutePath() + " - " + e);
		}

		return prevId;
	}

	// Return an array of "serviceAddresses"
	@Override
	public String[] query() {
		// Return an array of (decoded) filenames
		if (!serviceDir.exists()) {
			return new String[0];
		}
		File[] files = serviceDir.listFiles();
		String[] addrs = new String[files.length];
		for (int i = 0; i < addrs.length; ++i) {
			addrs[i] = decode(files[i].getName());
		}
		return addrs;
	}

	// Remove  the file "serviceAddress" and return its "instanceId"
	// Remove the directory when no instances left
	@Override
    public String unregister(String serviceAddress) {
        String instanceId = null;
        File instanceFile = new File(serviceDir, encode(serviceAddress));
        if (instanceFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(instanceFile))) {
                instanceId = reader.readLine();
            } catch (IOException e) {
                System.err.println("ERROR: Failed to read instanceId from registry file: " + instanceFile.getAbsolutePath() + " - " + e);
            } finally {
                instanceFile.delete();
            }
            if (serviceDir.list().length == 0) {
                // Could be a race if another thread is about to create an entry !
                serviceDir.delete();
            }
        }
        return instanceId;
    }

	// Encode/decode characters that are illegal in filenames.
	// Only '/' is illegal but must also %-encode '%'
    static String[] badChars = {
        "%", "%25",
        "/", "%2F"
    };

	private String encode(String name) {
		for (int i = 0; i < badChars.length; i += 2) {
			name = name.replaceAll(badChars[i], badChars[i+1]);
		}
		return name;
	}

	private String decode(String name) {
		for (int i = 0; i < badChars.length; i += 2) {
			name = name.replaceAll(badChars[i+1], badChars[i]);
		}
		return name;
	}

	/*=================================================================================*/

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: service-name address-prefix num");
			return;
		}
		ServiceRegistry reg = new FileServiceRegistry("/users/burn/REGISTRY");
		reg.setName(args[0]);
		String[] addrs = reg.query();
		System.out.println("Service " + args[0] + " has " + addrs.length + " instances");

		int num = Integer.valueOf(args[2]);
		for (int i = 0; i < num; ++i) {
			reg.register(args[1]+i, "100" + i);
			addrs = reg.query();
			System.out.println("Service " + args[0] + " has " + addrs.length
					+ " instances");
			for (String addr : addrs) {
				System.out.println("   instance: " + addr);
			}
		}

		for (int i = 0; i < num-1; ++i) {
			String addr = args[1] + i;
			String prev = reg.unregister(addr);
			System.out.println("Unregistered " + addr + " was " + prev);
		}
	}
}
