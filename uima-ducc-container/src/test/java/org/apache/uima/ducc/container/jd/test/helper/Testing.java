package org.apache.uima.ducc.container.jd.test.helper;

public class Testing {

	private static boolean disabled = true;
	
	private static boolean warned = false;
	
	public static boolean isDisabled(String name ) {
		if(disabled) {
			if(!warned) {
				System.err.println("Tests are disabled: "+name);
			}
			warned = true;
		}
		return disabled;
	}
}
