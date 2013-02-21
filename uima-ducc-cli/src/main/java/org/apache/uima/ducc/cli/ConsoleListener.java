
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
package org.apache.uima.ducc.cli;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.NodeIdentity;

class ConsoleListener
    implements Runnable
{
    private ServerSocket sock;
    private CliBase submit;
    private Map<Integer, StdioListener> listeners = new HashMap<Integer, StdioListener>();

    private int          console_listener_port;
    private String       console_host_address;

    private boolean      in_shutdown = false;
    private int          callers;   // number of remote processes we expect to listen for

    ConsoleListener(CliBase submit)
        throws Throwable
    {
        this.submit = submit;
        this.sock = new ServerSocket(0);
        this.console_listener_port  = sock.getLocalPort();

        NodeIdentity ni = new NodeIdentity();
        this.console_host_address = ni.getIp();            
        this.callers = 1;         // assume we'll get at least one listener, else we would not have been called.
    }

    String getConsoleHostAddress()
    {
        return console_host_address;
    }

    int getConsolePort()
    {
        return console_listener_port;
    }

    /**
     * The caller knows there may be more than one remote process calling us but
     * we've no clue when or if they will show up.  We assume here they do, and 
     * rely on some external influence to correct us if not.
     */
    synchronized void incrementCallers()
    {
        callers++;
    }

    synchronized void waitForCompletion()
    {
        try {
            while ( (callers > 0) && ( !in_shutdown) ) {
                wait();
            }
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private synchronized void releaseWait()
    {
        callers--;
        notify();
    }

    synchronized boolean isShutdown()
    {
        return in_shutdown;
    }

    void shutdown()
    {
        in_shutdown = true;
        try {
            sock.close();
            for ( StdioListener sl : listeners.values() ) {
                sl.close();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void delete(int port)
    {
        listeners.remove(port);
        if ( listeners.size() == 0 ) {
            synchronized(submit) {
                releaseWait();
            }
            shutdown();
        }
    }

    public void run()
    {
        System.out.println("Listening on " + console_host_address + " " + console_listener_port);

        while ( true ) {
            try {                    
                Socket s = sock.accept();
                StdioListener sl = new StdioListener(s, this);
                int p = s.getPort();
                listeners.put(p, sl);

                Thread t = new Thread(sl);
                t.start();                
            } catch (Throwable t) {
                if ( ! in_shutdown ) shutdown();
                return;
            }
        }
    }

    class StdioListener
        implements Runnable
    {
        Socket sock;
        InputStream is;
        boolean done = false;
        ConsoleListener cl;
        String remote_host;
        String leader;

        BufferedOutputStream logfile = null;
        String filename = null;
        static final String console_tag = "1002 CONSOLE_REDIRECT ";
        int tag_len = 0;
        boolean first_error = true;

        StdioListener(Socket sock, ConsoleListener cl)
        {
            this.sock = sock;
            this.cl = cl;

            InetAddress ia = sock.getInetAddress();
            remote_host = ia.getHostName();
            System.out.println("===== Listener starting: " + remote_host + ":" + sock.getPort());
            int ndx = remote_host.indexOf('.');
            if ( ndx >= 0 ) {
                // this is just for console decoration, keep it short, who cares about the domain
                remote_host = remote_host.substring(0, ndx);
            }
            leader = "[" + remote_host + "] ";
            tag_len = console_tag.length();
        }

        public void close()
            throws Throwable
        {
            System.out.println("===== Listener completing: " + remote_host + ":" + sock.getPort());
            this.done = true;
            is.close();
            cl.delete(sock.getPort());

            logfile.flush();
            logfile.close();
        }

        void tee(String leader, String line)
        {
            try {
				if ((logfile == null) && line.startsWith(console_tag)) {
					filename = line.substring(tag_len);
					logfile = new BufferedOutputStream(new FileOutputStream(filename));

                    System.out.println("Create logfile " + filename);
				}
				if (logfile != null) {
					logfile.write(leader.getBytes());
					logfile.write(' ');
					logfile.write(line.getBytes());
					logfile.write('\n');
                    logfile.flush();
				} else {
                    System.out.println("Bypass logfile");
                } 
			} catch (Exception e) {
                if ( first_error ) {
                    System.out.println("Cannot create or write log file[" + filename + "]: " + e.getMessage());
                    e.printStackTrace();
                }
                first_error = false;
			}
			System.out.println(leader + line);
        }

        /**
         * We received a buffer of bytes that needs to be put into a string and printed.  We want
         * to split along \n boundaries so we can insert the host name at the start of every line.
         *
         * Simple, except that the end of the buffer may not be \n, instead it could be the
         * start of another line.
         *
         * We want to save the partial lines as the start of the next line so they can all be
         * printed all nicely.
         */
        String partial = null;
        public void printlines(byte[] buf, int count)
        {
            String tmp = new String(buf, 0, count);
            String[] lines = tmp.split("\n");
            int len = lines.length - 1;
            if ( len < 0 ) {
                // this is a lone linend.  Spew the partial if it exists and just return.
                if ( partial != null ) {
                    tee(leader, partial);
                    partial = null;
                }
                return;
            }


            if ( partial != null ) {
                // some leftover, it's the start of the first line of the new buffer.
                lines[0] = partial + lines[0];
                partial = null;
            }

            for ( int i = 0; i < len; i++ ) {
                // spew everything but the last line
                tee(leader, lines[i]);
            }

            if ( tmp.endsWith("\n") ) {
                // if the last line ends with linend, there is no partial, just spew
                tee(leader, lines[len]);
                partial = null;
            } else {
                // otherwise, wait for the next buffer
                partial = lines[len];
            }
        }

        public void run()
        {            
            byte[] buf = new byte[4096];
            try {
                is = sock.getInputStream();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            
            try {
                int count = 0;
                while ( (count = is.read(buf)) > 0 ) {
                    printlines(buf, count);
                }
                System.out.println(leader + "EOF:  exiting");
            } catch ( Throwable t ) {
                t.printStackTrace();
            }
            try {
                close();
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

