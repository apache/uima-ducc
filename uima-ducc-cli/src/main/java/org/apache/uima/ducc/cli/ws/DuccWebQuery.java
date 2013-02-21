package org.apache.uima.ducc.cli.ws;

import java.net.InetAddress;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;

public class DuccWebQuery {
	
	protected DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	protected String DUCC_HOME = Utils.findDuccHome();
	protected String ws_scheme = "http";
	protected String ws_host = "localhost";
	protected String ws_port = "42133";
	protected String ws_servlet = null;;
	
	protected boolean showURL = true;
	
	protected DuccWebQuery(String servlet) {
		assert_ducc_home();
		determine_host();
		determine_port();
		ws_servlet = servlet;
	}
	
	protected void assert_ducc_home() {
		if(DUCC_HOME == null) {
			throw new RuntimeException("DUCC_HOME not specified");
		}
	}
	
	protected void determine_host() {
		try {
			ws_host = java.net.InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) {
		}
		String host = dpr.getProperty(DuccPropertiesResolver.ducc_ws_host);
		if(host != null) {
			ws_host = host;
		}
		if(ws_host != null) {
			if(ws_host.length() > 0) {
				if(!ws_host.contains(".")) {
					try {
						InetAddress addr = InetAddress.getLocalHost();
						String canonicalHostName = addr.getCanonicalHostName();
						if(canonicalHostName.startsWith(ws_host)) {
							ws_host = canonicalHostName;
						}
					}
					catch(Exception e) {
						
					}
				}
			}
		}
	}
	
	protected void determine_port() {
		String port = dpr.getProperty(DuccPropertiesResolver.ducc_ws_port);
		if(port != null) {
			ws_port = port;
		}
	}
	
	protected String getUrlString() {
		String urlString = ws_scheme+"://"+ws_host+":"+ws_port+ws_servlet;
		if(showURL) {
			System.out.println(urlString);
		}
		return urlString;
	}
}
