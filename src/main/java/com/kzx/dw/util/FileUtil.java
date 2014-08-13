/**
* @Title: FileUtil.java
* @Description: TODO(用一句话描述该文件做什么)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月12日 下午5:59:38 
 */
package com.kzx.dw.util;

import java.io.File;

import org.apache.log4j.Logger;

public class FileUtil {
	private static Logger logger = Logger.getLogger(FileUtil.class);
	
	//创建递归目录
	public static boolean mkDir(String path)
	{
		File file = new File(path);
		if(file.exists())
		{
			return true;
		}
		else
		{		
			mkDir(file.getParentFile().getAbsolutePath());
			if(!file.mkdir())
				return false;					
		}
		return true;
	}
	
	//创建递归目录
	public static boolean mkDir(String[] dir)
	{
		return mkDir(getDirPath(dir));
	}
		
	//判断文件是否存在
	public static boolean isExist(String[] dir,String filename)
	{
		File file = new File(getFilePath(dir, filename));
		return file.exists();
	}
	
	public static String getFilePath(String[] dir,String filename)
	{
		String path = "";
		if(OSUtil.isWin())
		{
			int i=0;
			for(String s: dir)
			{
				if(i>0)
					path += "\\" + s;
				else
					path+=s;
				i++;
			}
				
			path += "\\" + filename;
		}
		else
		{
			int i=0;
			for(String s: dir)
			{
				if(i>0)
					path += "/" + s;
				else
					path+=s;
				i++;
			}
			path += "/" + filename;
		}
		return path;
	}
	
	public static String getDirPath(String[] dir)
	{
		return getFilePath(dir,"");
	}
	
	
	//删除文件
	public static boolean delFile(String[] dir,String filename)
	{	
		File file = new File(getFilePath(dir, filename));
		return file.delete();
	}
	
	public static void main(String[] args) {
		if(!FileUtil.mkDir(new String[]{"/home/hdpusr/test2/","test3","test4"}))
			logger.info("creat dir false");
		if(!FileUtil.isExist( new String[]{"/home/hdpusr/test2/","test3","test4"}, "test.xml"))
			logger.info("path not exist");
		else
			logger.info("path is exist");
		
		logger.info("test");
	}
}
