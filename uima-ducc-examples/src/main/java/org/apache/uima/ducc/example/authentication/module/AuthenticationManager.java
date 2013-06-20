package org.apache.uima.ducc.example.authentication.module;

import org.apache.uima.ducc.common.authentication.AuthenticationResult;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.example.authentication.site.SiteSecurity;

public class AuthenticationManager implements IAuthenticationManager {

	private final String version = "example 1.0";
	
	@Override
	public String getVersion() {
		return version;
	}

	@Override
	public boolean isPasswordChecked() {
		return true;
	}

	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain,
			String password) {
		IAuthenticationResult authenticationResult = new AuthenticationResult();
		authenticationResult.setFailure();
		try {
			if(SiteSecurity.isAuthenticUser(userid, domain, password)) {
				authenticationResult.setSuccess();
			}
		}
		catch(Exception e) {
			//TODO
		}
		return authenticationResult;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain,
			Role role) {
		IAuthenticationResult authenticationResult = new AuthenticationResult();
		authenticationResult.setFailure();
		try {
			if(SiteSecurity.isAuthenticRole(userid, domain, role.toString())) {
				authenticationResult.setSuccess();
			}
		}
		catch(Exception e) {
			//TODO
		}
		return authenticationResult;
	}

}
