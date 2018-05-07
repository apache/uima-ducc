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

package org.apache.uima.ducc.container.sd;

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
 * 
 * A registered entry consists of:
 *  - a folder named for for the service in a shared base registry directory. 
 *  - a file in this folder whose name represents the address of the service instance,  
 *    holding any additional details about the service instance.
 *    
 * The service name and address are encoded into valid filenames.
 * A shutdown hook is created so entries are removed during normal process termination.
 *
 * Entries can only be be removed by their creator. Entries should have the same visibility as the
 * base registry directory. 
 *
 * NOTE: Alternative implementations could be supported by putting a class with the same name 
 * earlier in the classpath, or by replacing this one with a proxy that loads an arbitrary class.
 * 
 */
public class ServiceRegistry_impl implements ServiceRegistry {

  Map<String, Map<String, String>> serviceNameMap = new HashMap<String, Map<String, String>>();

  Map<String, String> serviceMap;

  private File registryDir;

  private static ServiceRegistry instance = new ServiceRegistry_impl();
  
  public static ServiceRegistry getInstance() {
    return instance;
  }
  
  public boolean initialize(String location) {
    registryDir = new File(location);
    if (!registryDir.exists()) {
      registryDir.mkdir();
    }
    return registryDir.canWrite();
  }

  // Create file "address" holding "details"
  // Return previous value if already registered, or null if not.
  @Override
  public String register(String name, String address, String details) {
    String prevDetails = null;
    File serviceDir = new File(registryDir, encode(name));
    if (!serviceDir.exists()) {
      serviceDir.mkdir();
    }
    // Could be a race condition if the dir is deleted at this time
    File instanceFile = new File(serviceDir, encode(address));
    if (instanceFile.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(instanceFile))) {
        prevDetails = reader.readLine();
      } catch (IOException e) {
        System.err.println("ERROR: Failed to read previous details when updating registry file: "
                + instanceFile.getAbsolutePath() + " - " + e);
        prevDetails = e.toString();
      }
    }
    try (PrintWriter writer = new PrintWriter(instanceFile)) {
      writer.println(details);
    } catch (FileNotFoundException e) {
      System.err.println("ERROR: Failed to create registry file: " + instanceFile.getAbsolutePath()
              + " - " + e);
    }

    // Ensure that this instance is unregistered when process ends
    Runtime.getRuntime().addShutdownHook(new UnregisterHook(this, name, address));
    
    return prevDetails;
  }

  // Return first address or block if none available
  @Override
  public synchronized String fetch(String name) {
    
    File serviceDir = new File(registryDir, encode(name));
    while(true) {
      if (serviceDir.exists()) {
        File[] files = serviceDir.listFiles();
        if (files.length > 0) {
          return decode(files[0].getName());
        }
      }
      try {
        System.out.println("!! fetch will try "+name+" again in 15 secs");
        Thread.sleep(15000);
      } catch (InterruptedException e) {
      }
    }
  }

  // Return an array of 2-element arrays holding address & details
  @Override
  public String[][] query(String name) {
    File serviceDir = new File(registryDir, encode(name));
    if (!serviceDir.exists()) {
      return new String[0][];
    }
    File[] files = serviceDir.listFiles();
    String[][] addrs = new String[files.length][];
    for (int i = 0; i < addrs.length; ++i) {
      String details;
      try (BufferedReader reader = new BufferedReader(new FileReader(files[i]))) {
        details = reader.readLine();
      } catch (IOException e) {
        details = "ERROR: Failed to read instanceId from registry file: " + files[i].getAbsolutePath() + " - " + e;
      }
      String[] adPair = new String[2];
      adPair[0] = decode(files[i].getName());
      adPair[1] = details;
      addrs[i] = adPair;
    }
    return addrs;
  }

  // Remove the file "name/address"
  // Remove the directory when no instances left
  @Override
  public boolean unregister(String name, String address) {
    File serviceDir = new File(registryDir, encode(name));
    if (serviceDir.exists()) {
      File instanceFile = new File(serviceDir, encode(address));
      if (instanceFile.exists()) {
        instanceFile.delete();
        if (serviceDir.list().length == 0) {
          // Could be a race if another thread is about to create an entry !
          serviceDir.delete();
        }
        return true;
      }
    }
    return false;
  }

  // Encode/decode characters that are illegal in filenames.
  // Only '/' is illegal but must also %-encode '%'
  static String[] badChars = { "%", "%25", "/", "%2F" };

  private String encode(String name) {
    for (int i = 0; i < badChars.length; i += 2) {
      name = name.replaceAll(badChars[i], badChars[i + 1]);
    }
    return name;
  }

  private String decode(String name) {
    for (int i = 0; i < badChars.length; i += 2) {
      name = name.replaceAll(badChars[i + 1], badChars[i]);
    }
    return name;
  }

  private class UnregisterHook extends Thread {
    private ServiceRegistry registry;
    private String name;
    private String address;
    UnregisterHook(ServiceRegistry registry, String name, String address) {
      this.registry = registry;
      this.name = name;
      this.address = address;
    }
    public void run() {
      registry.unregister(name, address);
      System.out.println("!! Unregistered " + address + " in shotdown hook");
    }
  }

  /* ================================================================================= */

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 3) {
      System.out.println("Usage: service-name address-prefix num");
      return;
    }
    String name = args[0];
    ServiceRegistry reg = ServiceRegistry_impl.getInstance();
    String[][] addrs = reg.query(name);
    System.out.println("Service " + name + " has " + addrs.length + " instances");

    int num = Integer.valueOf(args[2]);
    for (int i = 0; i < num; ++i) {
      reg.register(name, args[1] + i, "100" + i);
      addrs = reg.query(name);
      System.out.println("Service " + args[0] + " has " + addrs.length + " instances");
      for (String[] addrDetails : addrs) {
        System.out.println("   addr: " + addrDetails[0] + " details: " + addrDetails[1]);
      }
    }

    String address = reg.fetch(name);
    System.out.println("fetch returned: " + address);
    
    System.out.println("Sleeping for 30 secs");
    Thread.sleep(30000);
    for (int i = 1; i < num + 1; ++i) {
      String addr = args[1] + i;
      boolean ok = reg.unregister(name, addr);
      System.out.println("Unregistered " + addr + " was " + ok);
    }
    
    address = reg.fetch(name);
    System.out.println("fetch returned: " + address);
    
    String addr = args[1] + 0;
    reg.unregister(name, addr);
    
    address = reg.fetch(name);
    System.out.println("fetch returned: " + address);
    
  }

}
