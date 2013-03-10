package org.apache.uima.ducc.cli;

import java.util.List;
import java.util.Properties;

public class DuccProcessCancel extends DuccServiceCancel
{

	public DuccProcessCancel(String [] args) 
        throws Exception
    {
        super(args);
	}

	public DuccProcessCancel(List<String> args) 
        throws Exception
    {
        super(args);
	}

	public DuccProcessCancel(Properties props) 
        throws Exception
    {
        super(props);
	}

    public boolean isService()
    {
        return false;
    }
			
}
