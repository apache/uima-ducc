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
package org.apache.uima.ducc.common.head;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public class DuccHead implements IDuccHead {

  private static IDuccHead instance = new DuccHead();
  
  public static IDuccHead getInstance() {
    return instance;
  }
  
  private DuccLogger logger = null;

  private AtomicBoolean isActive = new AtomicBoolean(true);

  private String duccHeadIp = "unknown";

  private boolean reliable = false;
  
  /*
   * create instance of this class
   */
  public DuccHead() {
    logger = DuccLogger.getLogger(DuccHead.class);
    init();
  }

  /*
   * initialize
   */
  private void init() {
    String location = "init";

    // Assume reliable if alternative head nodes specified
    // If the keepalived configuration is incorrect it will always be "backup"
    DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
    reliable = dpr.getProperty(DuccPropertiesResolver.ducc_head_reliable_list).trim().length() > 0;

    if (reliable) {
      String duccHead = dpr.getProperty(DuccPropertiesResolver.ducc_head);
      try {
        InetAddress inetAddress = InetAddress.getByName(duccHead);
        duccHeadIp = inetAddress.getHostAddress();
      } catch (UnknownHostException e) {
        logger.error(location, null, "No IP address found for: " + duccHead);
      }
      isActive.set(isVirtualMaster());
    }

    logger.info(location, null, "Initial state: ", reliable ? (isActive.get() ? "MASTER" : "BACKUP") : "not reliable (single head node)");
  }

  /* 
   * true if a "reliable" installation, i.e. multiple head nodes
   */
  public boolean isReliable() {
    return reliable;
  }

  /*
   * true if this node is currently in charge 
   * i.e. is the only head node or is the master in a multi-head installation
   */
  public boolean isActive() {
    return isActive.get();
  }

  /*
   * get ducc head mode state { result expected is "master" or "backup" or "unspecified"}
   */
  @Override
  public String get_ducc_head_mode() {
    return !isReliable() ? "unspecified" : (isActive() ? "master" : "backup");
  }

  /*
   * true if "master" or "backup"
   */
  @Override
  public boolean is_ducc_head_reliable() {
    return isReliable();
  }

  /*
   * true if "master"
   */
  @Override
  public boolean is_ducc_head_master() {
    return isReliable() && isActive();
  }

  /*
   * true if "master" or single-head-node
   */
  @Override
  public boolean is_ducc_head_virtual_master() {
    return isActive();
  }

  /*
   * true if "backup"
   */
  @Override
  public boolean is_ducc_head_backup() {
    return !isActive();
  }

  /*
   * Check if a transition has occurred. 
   * Called when a daemon receives a publication that it should act on or ignore. 
   * This is the only place the master/backup state is updated after initialization.
   */
  @Override
  public DuccHeadTransition transition() {
    if (!reliable) {
      return DuccHeadTransition.unspecified;
    }
    boolean prev = isActive.get();
    boolean curr = isVirtualMaster();
    if (curr == prev) {
      return curr ? DuccHeadTransition.master_to_master : DuccHeadTransition.backup_to_backup;
    } else {
      isActive.set(curr);
      logger.info("transition", null, "Switching to", curr ? "MASTER" : "BACKUP");
      return curr ? DuccHeadTransition.backup_to_master : DuccHeadTransition.master_to_backup;
    }
  }

  /*
   * For a reliable installation check if the virtual IP address currently belongs to this node
   */
  private boolean isVirtualMaster() {
    String[] cmd = { "/sbin/ip", "addr", "list" };
    String result = runCmd(cmd);
    return result.contains(duccHeadIp);
  }

  /*
   * execute a system command
   */
  private String runCmd(String[] command) {
    String location = "runCmd";
    String retVal = null;
    try {
      ProcessBuilder pb = new ProcessBuilder(command);
      Process p = pb.start();
      InputStream pOut = p.getInputStream();
      InputStreamReader isr = new InputStreamReader(pOut);
      BufferedReader br = new BufferedReader(isr);
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = br.readLine()) != null) {
        sb.append(line);
        logger.debug(location, null, line);
      }
      retVal = sb.toString();
      int rc = p.waitFor();
      logger.debug(location, null, rc);
    } catch (Exception e) {
      logger.error(location, null, e);
    }
    return retVal;
  }

}
