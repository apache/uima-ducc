package org.apache.uima.ducc.agent;

public class Test {

	public static void main(String[] args) {
		Killer killer = Killer.getInstance();
		String user = "foobar";
		String pid = "12345";
		//int rc = killer.kill(user, pid);
		//System.out.println(rc);
		boolean isKilled = killer.isKilled(user, pid);
		if(isKilled) {
			System.out.println("killed");
		}
		else {
			System.out.println("alive");
		}
	}

}
