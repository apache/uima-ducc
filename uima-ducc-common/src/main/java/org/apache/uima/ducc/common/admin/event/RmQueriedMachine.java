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
package org.apache.uima.ducc.common.admin.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RmQueriedMachine
	implements Serializable
{
	private static final long serialVersionUID = -8101741014979144426L;

    String name;
    String nodepoolId;
    long memory;
    int order;
    boolean blacklisted;                                         // UIMA-4142
    boolean online;                                              // UIMA-4234
    boolean responsive;                                          // UIMA-4234

    List<RmQueriedShare> shares = null;
    
    // UIMA-4142, account for blacklist
    public RmQueriedMachine(String name, String nodepoolId, long memory, int order, boolean blacklisted)
    {
        this.name = name;
        this.nodepoolId = nodepoolId;
        this.memory = memory;
        this.order = order;
        this.blacklisted = blacklisted;
        this.online = true;
        this.responsive = true;
    }

    public void setOffline()      { this.online = false; }       // UIMA-4234
    public void setUnresponsive() { this.responsive = false; }   // UIMA-4234

    public void addShare(RmQueriedShare rqs)
    {
        if ( shares == null ) shares = new ArrayList<RmQueriedShare>();
        shares.add(rqs);
    }

    public String getId()           { return name; }
    public long getMemory()         { return memory; }
    public int getShareOrder()      { return order; }
    public boolean isBlacklisted()  { return blacklisted; }        // UIMA-4142
    public boolean isOnline()       { return online; }             // UIMA-4142
    public boolean isResponsive()   { return responsive; }         // UIMA-4142
    
    static String fmt_s = "%20s %11s %6s %6s %15s %10s %5s %4s %s";
    String        fmt_d = "%20s %11s %6s %6s %15s %10d %5d %4d" ;

    public static String header()
    {
        return String.format(fmt_s, "Node", "Blacklisted", "Online", "Status", "Nodepool", "Memory", "Order", "Free", "Shares");
    }

    public static String separator()
    {
        return String.format(fmt_s, "--------------------", "-----------", "------", "------", "---------------", "----------", "-----", "----", "----------");
    }

    public String toConsole() 
    {
        StringBuffer sb = new StringBuffer();

        if ( shares == null ) {
            sb.append(String.format(fmt_d, name, blacklisted, online, (responsive ? "up" : "down"), nodepoolId, memory, order, 0));
            sb.append(" [none]");
        } else {
            int used = 0;
            for ( RmQueriedShare s : shares ) {                
                used += s.getShareOrder();
            }
            sb.append(String.format(fmt_d, name, blacklisted, online, (responsive ? "up" : "down"), nodepoolId, memory, order, order - used));

            String spacer = " ";
            String altSpacer = "\n" + String.format(fmt_s, "", "", "", "", "", "", "", "", ""); // yes, blank, of exactly the right size
            for ( RmQueriedShare s : shares ) {                
                sb.append(spacer);
                sb.append(s.toConsole());
                spacer = altSpacer;
            }
        }
        return sb.toString();

    }

    public String toCompact() 
    {
        // name memory order(nqshares) unused-shares share-details...
        StringBuffer sb = new StringBuffer();
        sb.append(name);
        sb.append(" ");
        sb.append(nodepoolId);
        sb.append(" ");
        sb.append(Boolean.toString(blacklisted));
        sb.append(" ");
        sb.append(Boolean.toString(online));
        sb.append(" ");
        sb.append(responsive ? "up" : "down");
        sb.append(" ");
        sb.append(Long.toString(memory));
        sb.append(" ");
        sb.append(Integer.toString(order));
        
        if ( shares == null ) {
            sb.append(" 0, None");
        } else {
            int used = 0;
            for ( RmQueriedShare s : shares ) {                
                used += s.getShareOrder();
            }

            sb.append(" ");
            sb.append(Integer.toString(order - used));
            for ( RmQueriedShare s : shares ) {
                sb.append(",");
                sb.append(s.toCompact());
            }
        }
        return sb.toString();
    }

    public String toString()
    {
        return String.format("%14s %5s %12d %3d %3d", name, blacklisted, memory, order, shares == null ? 0 : shares.size());
    }
}
