/**
* @Title: OSUtil.java
* @Description: TODO(识别操作系统类型)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月13日 上午10:40:14 
 */
package com.kzx.dw.util;

import java.util.Properties;

public class OSUtil {
	private static String  getOSType()
	{
		Properties prop = System.getProperties();
		String os = prop.getProperty("os.name");
		return os;
	}
	
	public static boolean isWin()
	{
		String os = getOSType();
		if(os.startsWith("win") || os.startsWith("Win"))
		{
			return true;
		}
		else
			return false;
	}
	
	public static boolean isLinux()
	{
		String os = getOSType();
		if(os.startsWith("linux") || os.startsWith("Linux"))
		{
			return true;
		}
		else
			return false;
		
		
		
	}
	
	public static void main(String[] args) {
		String outputtable="person||String name;String prov;int age||/temp/fs";

		
		for(String tablestring: outputtable.split("\\|\\|"))
		{
			System.out.println("table=" + tablestring);
		}	
	}
}