package org.apache.uima.ducc.ws;

public class Helper {

	/**
	 * Convert String to long, else zero
	 */
	public static long String2Long(String value) {
		long retVal = 0;
		try{
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
}
