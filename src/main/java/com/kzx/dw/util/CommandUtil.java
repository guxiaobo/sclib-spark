/**
* @Title: CommandUtil.java
* @Description: TODO(执行命令行的工具类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月6日 下午2:41:22 
 */
package com.kzx.dw.util;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;



public class CommandUtil {
	
	/*
     *执行命令行
     */
	public static String execute(String sendCommand) {
		try {
			Runtime rt = Runtime.getRuntime();
			String command = "/bin/bash";
			Process child = rt.exec(command);
			OutputStream out = child.getOutputStream();
			OutputStreamWriter writer = null;
			try {
				writer = new OutputStreamWriter(child.getOutputStream(), "utf8");
				writer.write(sendCommand);
				writer.flush();
				out.close();
				InputStreamReader isr = new InputStreamReader(child.getInputStream(), "utf8");
				LineNumberReader input = new LineNumberReader(isr);
				String line = "";
				String resultStr = "";
				while ((line = input.readLine()) != null) {
					 resultStr += line + "\n";
				}		
				input.close();
				InputStreamReader isrError = new InputStreamReader(child.getErrorStream(), "utf8");
				LineNumberReader error = null;
				try {
					error = new LineNumberReader(isrError);
					while ((line = error.readLine()) != null) {
						 resultStr += line + "\n";
					}
					child.waitFor();
				} finally {
					if (null != error) {
						error.close();
					}
				}
				return resultStr;
			} finally {
				if (null != writer) {
					writer.close();
				}
			}

		} catch (Exception e) {
			LogUtil.UTILLOG.error("CommandUtil#execute error" ,e );
		}
		
		
		return "fail";
	}
    
    
    public static void main(String[] args) throws IOException {
    	System.out.println(execute("ls -lh"));
   }
}