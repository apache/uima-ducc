package org.apache.uima.ducc.example.authentication.site;

public class SiteSecurity {

	public static boolean isAuthenticUser(String userid, String domain,
			String password) {
		return true;
	}
	
	public static boolean isAuthenticRole(String userid, String domain,
			String role) {
		return true;
	}
}
