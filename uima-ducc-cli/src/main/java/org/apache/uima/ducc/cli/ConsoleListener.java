
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.Pair;

class ConsoleListener
    implements Runnable
{
    private ServerSocket sock;
    private CliBase submit;
    private Map<Integer, Pair<StdioReader, StdioWriter>> listeners = new HashMap<Integer, Pair<StdioReader, StdioWriter>>();

    private int          console_listener_port;
    private String       console_host_address;

    private boolean      in_shutdown = false;
    private boolean      start_stdin = false;
    private int          numConnected = 0;
    
    private IDuccCallback consoleCb;
    // private int          callers;   // number of remote processes we expect to listen for

    boolean debug = false;
    private boolean suppress_log;
    
    ConsoleListener(CliBase submit, IDuccCallback consoleCb)
        throws Exception
    {
        this.submit = submit;
        this.sock = new ServerSocket(0);
        this.console_listener_port  = sock.getLocalPort();
        this.consoleCb = consoleCb;
        
        NodeIdentity ni = new NodeIdentity();
        this.console_host_address = ni.getIp();            

        debug = submit.debug; //isDebug();
        suppress_log = submit.suppress_console_log;
    }

    String getConsoleHostAddress()
    {
        return console_host_address;
    }

    int getConsolePort()
    {
        return console_listener_port;
    }

    synchronized boolean isShutdown()
    {
        return in_shutdown;
    }

    void shutdown()
    {
        if ( debug ) System.out.println("Console handler: Shutdown starts");
        in_shutdown = true;
        try {
            sock.close();
            for ( Pair<StdioReader, StdioWriter> handler: listeners.values() ) {
                handler.first().shutdown();
                handler.second().shutdown();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void delete(int port)
    {
        int count;
        synchronized(this) {
            Pair<StdioReader, StdioWriter> listener = listeners.remove(port);
            if ( listener != null ) {
                try {
                    listener.first().shutdown();
                    listener.second().shutdown();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            count = listeners.size();
        }

        if ( debug ) System.out.println("Console handler: Removed handler for port " + port + ", size = "  + listeners.size());
        if ( count == 0 ) {
            shutdown();
        }
    }

    void startStdin(boolean start_stdin)
    {
        this.start_stdin = start_stdin;
    }

    public void run()
    {
        if ( debug ) System.out.println("Listening on " + console_host_address + " " + console_listener_port);

        while ( true ) {
            try {                    
                Socket s = sock.accept();
                StdioReader sr = new StdioReader(s);
                StdioWriter sw = new StdioWriter(s);
                int p = s.getPort();
                synchronized(this) {
                    listeners.put(p, new Pair<StdioReader, StdioWriter>(sr, sw));
                    sr.idNum = ++numConnected;
                }

                Thread t = new Thread(sr, "STDOUT");
                t.start();                

                if ( start_stdin ) {
                    // generally started only for AP (ducclet)
                    Thread tt = new Thread(sw, "STDIN");
                    tt.start();             
                }   
            } catch (Throwable t) {
                if ( ! in_shutdown ) shutdown();
                if ( debug ) System.out.println("console listener returns");
                submit.consoleExits();
                return;
            } 
        }
    }

    class StdioReader
        implements Runnable
    {
        Socket sock;
        InputStream is;
        boolean shutdown = false;
        ConsoleListener cl;
        String remote_host;
        private PrintWriter logout = null;

        static final String console_tag = "1002 CONSOLE_REDIRECT ";
        int tag_len = 0;
        private int idNum;

        StdioReader(Socket sock)
        {
            this.sock = sock;

            InetAddress ia = sock.getInetAddress();
            remote_host = ia.getHostName();
            tag_len = console_tag.length();

            if ( debug ) System.out.println("===== Listener starting: " + remote_host + ":" + sock.getPort());
        }

        public void shutdown()
            throws Exception
        {
            if ( shutdown ) return;  // idempotency, things can happen in all sorts of orders
            if ( debug ) System.out.println("===== Listener completing: " + remote_host + ":" + sock.getPort());
            shutdown = true;
            is.close();
            if (logout != null) {
              logout.close();
            }
            // Tell the ConsoleListener that it should shutdown all listeners on this port (self included!)
            delete(sock.getPort());
        }

        // When not saving a log file stream all of the console back to the caller
        boolean do_console_out = suppress_log;

        void doWrite(String line)
        {
            if ( line.startsWith(console_tag) && !suppress_log) {
                String logfile = line.substring(tag_len);
                try {
                  logout = new PrintWriter(logfile);
                  return;
                } catch (FileNotFoundException e) {
                  consoleCb.status("Failed to create log file: " + logfile);
                  e.printStackTrace();
                }
            }
            if (logout != null) {
              logout.println(line);
            }
            if ( do_console_out ) {
                consoleCb.console(idNum, line);
            } else {
                if ( line.startsWith("1001 Command launching...")) {
                  do_console_out = true;
                }
            }
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
                    doWrite(partial);
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
                doWrite(lines[i]);
            }

            if ( tmp.endsWith("\n") ) {
                // if the last line ends with linend, there is no partial, just spew
                doWrite(lines[len]);
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
                if ( debug ) System.out.println(remote_host + ": EOF:  exiting");
            } catch ( Throwable t ) {
                t.printStackTrace();
            } finally {
                try {
                    shutdown();
                } catch (Throwable e) {
                    // crash here, don't care, can't do anything about it
                }
            }
        }
    }

    class StdioWriter
        implements Runnable
    {
        Socket sock;
        OutputStream out;

        boolean done = false;
        boolean is_shutdown = false;

        StdioWriter(Socket sock)
        {
            this.sock = sock;
        }

        synchronized void shutdown()
        {
            is_shutdown = true;
        }

        private void close()
        {
            try {
                if ( out != null ) {
                    out.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void run()
        {
            if ( debug ) System.out.println("STDIN LISTENER STARTS *******");
            try {
                out = sock.getOutputStream();
            } catch (Exception e) {
                System.out.println("Cannot acquire remote socket for stdin redirection: " + e.toString());
                return;
            }


            byte[] buf = new byte[4096];
            int dbg = 0;
            try {
                while ( true ) {
                    int cnt = System.in.available();
                    if ( cnt > 0 ) {
                        while ( cnt > 0) {
                            int min = Math.min(cnt, buf.length);
                            System.in.read(buf, 0, min);
                            out.write(buf, 0, min);
                            cnt -= min;
                        }
                    } else {
                        synchronized(this) {
                            if ( is_shutdown ) break;
                        }
                        try {
                            Thread.sleep(100);
                            if ( ++dbg % 100 == 0 ) {
                                if ( debug ) System.out.println("STDIN: Sleep: " + dbg);
                            }
                        } catch ( InterruptedException e ) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("Error in process stdin redirection - redirection ended. " + e.toString());
            } finally {
                close();
            }
            if ( debug ) System.out.println("***********STDIN returns");
        }
    }
}

