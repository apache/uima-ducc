package org.apache.uima.ducc.ws.server.nodeviz;

public class SpectrumColorMap { 

    /*
     * calculate colors for index colind
     */
    public static  String color (int colind)    {
    	colind = Math.abs(colind) % 512;
        int i1 = (colind % 8) 			* 32;
        int i2 = ((colind / 8) % 8) 	* 32;
        int i3 = ((colind / 64) % 8) * 32;
        return  i1 + "," + i2 + ","  + i3;   
    }
    
    /*
     * returns the regular color;
     * however, if the color is too dark, we increase the brightness by 2
     */
    public static  String neverDarkColor (int colind)    {
    	colind = Math.abs(colind) % 512;
    	int i1 = (colind % 8)        * 32;
    	int i2 = ((colind / 8) % 8)  * 32;
    	int i3 = ((colind / 64) % 8) * 32;
    	if (i1+i2+i3 < 60) {
    		i1 *=2 ; i2 *=2; i3 *=2;
    	}
    	return  i1 + "," + i2 + ","  + i3;   
//    
    }
    
    /*
     * returns the bit lighter than the regular color;
     * however, if the color is too dark, we increase the brightness by 2
     */
    public static  String neverDarkColorAlmostLight (int colind)    {
    	colind = Math.abs(colind) % 512;
    	int i1 = (colind % 8)        * 28 + 44;
    	int i2 = ((colind / 8) % 8)  * 28 + 44;
    	int i3 = ((colind / 64) % 8) * 28 + 44;
    	if (i1+i2+i3 < 60) {
    		i1 *=2 ; i2 *=2; i3 *=2;
    	}
    	return  i1 + "," + i2 + ","  + i3;   
//    
    }

    public static  String darkColor (int colind)    {
    	colind = Math.abs(colind) % 512;
    	int i1 = (colind % 8)        * 20;
    	int i2 = ((colind / 8) % 8)  * 20;
    	int i3 = ((colind / 64) % 8) * 20;
    	return  i1 + "," + i2 + ","  + i3;   
    }
    
    public static String lightColor (int colind)     {
    	colind = Math.abs(colind) % 512;
        int i1 = (colind % 8)          * 24  +87;
        int i2 = ((colind / 8) % 8)    * 24  +87;
        int i3 = ((colind / 64) % 8)   * 24  +87;
        return  i1 + "," + i2 + ","  + i3;   
    }
    public static String veryLightColor (int colind)   {
    	colind = Math.abs(colind) % 512;
        int i1 = (colind  % 8)  		* 8 + 199;
        int i2 = ((colind / 8)  % 8)    * 8 + 199;
        int i3 = ((colind / 64) % 8)    * 8 + 199;      
        return  i1 + "," + i2 + ","  + i3;  
    }
    
    /*
     * calculate colors for a String s
     */
 
    public static String color (String s)    {
    	if (s == null)
    		return color(0);
    	else {
    		return color(s.hashCode());
    	}
    }
    
    public static String neverDarkColor (String s)    {
    	if (s == null)
    		return color(0);
    	else {
    		return neverDarkColor(s.hashCode());
    	}
    }
    
    public static String neverDarkColorAlmostLight (String s)    {
    	if (s == null)
    		return color(0);
    	else {
    		return neverDarkColorAlmostLight(s.hashCode());
    	}
    }
    
    public static String lightColor (String s)    {
    	if (s == null)
    		return lightColor(0);
    	else
    		return lightColor(s.hashCode()); //is a prime
    }
    
    public static String darkColor (String s)    {
    	if (s == null)
    		return darkColor(0);
    	else
    		return darkColor(s.hashCode()); //is a prime
    }
    
    public static String veryLightColor (String s)    {
    	if (s == null)
    		return veryLightColor(0);
    	else
    		return veryLightColor(s.hashCode()); //is a prime
    }
}
