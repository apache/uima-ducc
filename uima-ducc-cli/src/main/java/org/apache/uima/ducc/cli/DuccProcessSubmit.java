package org.apache.uima.ducc.cli;

import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;

public class DuccProcessSubmit extends DuccServiceSubmit
{	
    public int run(String[] args) 
        throws Exception 
    {
        super.setServiceType(ServiceDeploymentType.other);
        return super.run(args);
    }	

	public static void main(String[] args) {
		try {
			DuccProcessSubmit dps = new DuccProcessSubmit();
			int rc = dps.run(args);
            System.exit(rc == 0 ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(1);
		}
	}

}
