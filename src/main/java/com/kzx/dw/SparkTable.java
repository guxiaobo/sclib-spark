/**
* @Title: SparkTable.java
* @Description: TODO(Spark中table类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月7日 下午2:56:45 
 */
package com.kzx.dw;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.kzx.dw.util.CommandUtil;
import com.kzx.dw.util.FileUtil;
import com.kzx.dw.util.JavaSourceUtil;
import com.kzx.dw.util.OSUtil;

public class SparkTable {
	
	protected static Logger logger = Logger.getLogger(SparkTable.class);
	
	private String dbname;
	private String tablename;
	private List<String> fieldName = new ArrayList<String>();
	private List<String> fieldType = new ArrayList<String>();
	private String rootDir;
	

	public SparkTable(String dbname, String tablename, String tabledefine, String rootDir) throws SclibException
	{
		this.dbname = dbname;
		this.tablename = tablename;	
		this.rootDir = rootDir;	
		Set<String> defineSet = new HashSet<String>();       
        for(String segment : tabledefine.split(";"))
        {	        	
        	String []s = segment.split(" ");
        	if(s.length==2)
        	{
        		if(defineSet.contains(s[1]))
        		{
        			throw new SclibException("SparkTable create error: hava same field");
        		}
        		defineSet.add(s[1]);
        		fieldName.add(s[1]);
        		fieldType.add(s[0]);
        	}	
        } 
	}
	
	
	public void createTable() throws SclibException
	{
		//生成table对应的jar包       
	    String source = JavaSourceUtil.makeJavaSource(fieldName, fieldType ,tablename, getPackageName());
	    logger.info(source);
	    byte[] data = JavaSourceUtil.compile(tablename, source); 
		try 
		{
		    logger.debug(getTableClassAbsolutePath());
			File f=new File(getTableClassAbsolutePath());
		    FileOutputStream fos=new FileOutputStream(f);
		    fos.write(data);
		    fos.close();
		    if(OSUtil.isWin())
		    {
		    	logger.debug("cd /d " + rootDir +" && jar cvf " + getTableJarPath()  +" " + getTableClassPath());
		    	logger.debug(CommandUtil.execute("cd /d " + rootDir +" && jar cvf " + getTableJarPath()  +" " + getTableClassPath()));
		    }
		    else
		    {
		    	logger.debug("cd " + rootDir +";jar cvf " + getTableJarPath()  +" " + getTableClassPath());
		    	logger.debug(CommandUtil.execute("cd " + rootDir +";jar cvf " + getTableJarPath()  +" " + getTableClassPath()));
		    }   
		} catch (Exception e) {
			logger.error(e);
		}  
	}
               

	//创建递归目录
	private static void mkDir(File file)
	{
		if(file.getParentFile().exists())
		{
			file.mkdir();
		}else
		{
			mkDir(file.getParentFile());
			file.mkdir();
		}
	}
	
	//得到表对应的包名 
	private String getPackageName()
	{
		return dbname;
	}
	
	//得到表对应Class全名 
	public String getTableClassName()
	{
		return dbname + "." + tablename;
	}
	

	//得到表对应class文件相对路径
	private String getTableClassPath()
	{
		return FileUtil.getFilePath(new String[]{dbname}, tablename+".class");
	}
	
	//得到表对应class文件绝对路径
	private String getTableClassAbsolutePath()
	{
		return FileUtil.getFilePath(new String[]{rootDir,dbname}, tablename+".class");
	}
	
	//得到表对应jar文件相对路径
	private String getTableJarPath()
	{
		return FileUtil.getFilePath(new String[]{dbname}, tablename+".jar");
	}
	
	//得到表对应jar文件绝对路径
	public String getTableJarAbsolutePath()
	{
		return FileUtil.getFilePath(new String[]{rootDir,dbname}, tablename+".jar");
	}
	
	public String getDbname() {
		return dbname;
	}
	public void setDbname(String dbname) {
		this.dbname = dbname;
	}
	public String getTablename() {
		return tablename;
	}
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	public List<String> getFieldName() {
		return fieldName;
	}
	public void setFieldName(List<String> fieldName) {
		this.fieldName = fieldName;
	}
	public List<String> getFieldType() {
		return fieldType;
	}
	public void setFieldType(List<String> fieldType) {
		this.fieldType = fieldType;
	}
	
	
}

