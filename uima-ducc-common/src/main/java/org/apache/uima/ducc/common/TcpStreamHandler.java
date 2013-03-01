package org.apache.uima.ducc.common;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;


public class TcpStreamHandler
    extends URLStreamHandler
{
    public TcpStreamHandler() {}
    
    public URLConnection openConnection(URL u)
    {
        //throw new Exception("This protocol handler isn't expected to actually work.");
        return null;
    }
}
