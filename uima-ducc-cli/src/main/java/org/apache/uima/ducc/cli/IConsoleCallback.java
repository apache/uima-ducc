package org.apache.uima.ducc.cli;

public interface IConsoleCallback
{
    public void stdout(String host, String filename, String msg);
    public void stderr(String host, String filename, String msg);
}
