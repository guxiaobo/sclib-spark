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

import com.kzx.dw.util.CommandUtil;
import com.kzx.dw.util.JavaSourceUtil;
import com.kzx.dw.util.LogUtil;

public class SparkTable {
	
	private String dbname = "bean";
	private String tablename;
	private List<String> fieldName = new ArrayList<String>();
	private List<String> fieldType = new ArrayList<String>();
	private static final String TABLEDIR="/tmp/sclib/";
	private static final String TABLEPACKAGE="tmp.sclib.";
	
	
	public SparkTable(String tablename, String tabledefine) throws SclibException
	{
		this.tablename = tablename;	
		init(tabledefine);
	}
	
	public SparkTable(String dbname, String tablename, String tabledefine) throws SclibException
	{
		this.dbname = dbname;
		this.tablename = tablename;	
		init(tabledefine);
		
	}
	
	private void init(String tabledefine) throws SclibException
	{
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
	
	public void createTable()
	{
        //生成table对应的jar包       
	    String source = JavaSourceUtil.makeJavaSource(fieldName, fieldType ,tablename, getPackageName());	      
	    byte[] data = JavaSourceUtil.compile(tablename, source); 
		try 
		{
		    File f=new File(getClassDir());
		    if(!f.isDirectory())
		    {
		    	mkDir(f);
		    }

		    f=new File(getClassPath());
		    if(!f.exists())
		    {
		    	f.createNewFile();
		    }
		    else
		    {
		    	f.delete();
		    	f.createNewFile();
		    }
		    		               
		    FileOutputStream fos=new FileOutputStream(f);
		    fos.write(data);
		    fos.close();
		    CommandUtil.execute("rm "+ getJarPath());
		    System.out.println("rm "+ getJarPath());
		    System.out.println("cd " + TABLEDIR +";jar cvf " + getJarPath()  +" " + getClassPath());
		    System.out.println(CommandUtil.execute("jar cvf " + getJarPath()  +" " + getClassPath()));
			       
		} catch (Exception e) {
			LogUtil.UTILLOG.error("JavaSourceUtil#getJarPath error" ,e );
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
		return TABLEPACKAGE + dbname;
	}
	
	//得到表对应Class全名 
	public String getClassName()
	{
		return TABLEPACKAGE + dbname + "." + tablename;
	}
	
	//得到表对应class文件的目录名字 
	private String getClassDir()
	{
		return TABLEDIR + dbname;
	}
			
	//得到表对应class文件路径
	private String getClassPath()
	{
		return TABLEDIR + dbname + "/" + tablename + ".class";
	}
	
	//得到表对应jar文件路径
	private String getJarPath()
	{
		return TABLEDIR + dbname + "/" + tablename + ".jar";
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

