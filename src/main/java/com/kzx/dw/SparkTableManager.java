/**
* @Title: SparkTableManager.java
* @Description: TODO(Spark Table管理类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月7日 下午3:02:21 
 */
package com.kzx.dw;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.kzx.dw.util.FileUtil;
import com.kzx.dw.util.OSUtil;

public class SparkTableManager {
	
	private  Map<String, SparkTable> sparkTableMap = new HashMap<String,SparkTable>();
	private  String rootDir;
	private  JavaSQLContext stx;
	private  JavaSparkContext sc;
	private static Logger logger = Logger.getLogger(SparkTableManager.class);
	
	public SparkTableManager(JavaSparkContext sc,JavaSQLContext stx)
	{
		this.sc = sc;
		this.stx = stx;
	}
	public SparkTableManager()
	{
	}
	
	
	public void init(String path) throws SclibException
	{
		if(path.endsWith("/")||path.endsWith("\\"))
		{
			rootDir = path.substring(0,path.length()-2);
		}
		
		else
			rootDir = path;
		
		if(!FileUtil.mkDir(rootDir))
		{
			throw new SclibException("spark table目录初始化失败");
		}
	}
	
	public void register(String dbname,String tablename, String tabledefine, boolean overwrite) throws SclibException
	{
		register(dbname, tablename,tabledefine,null,overwrite);
	}
	
	
	public void createTableJar(String dbname,String tablename, String tabledefine, String path, boolean overwrite) throws SclibException
	{
		if(overwrite)
		{
			delTable(dbname, tablename);
		}
		
		SparkTable table = new SparkTable(dbname,tablename, tabledefine,rootDir);
		if(!isExistTable(dbname, tablename))
		{
			if(!FileUtil.mkDir(new String[]{rootDir,dbname}))
			{
				throw new SclibException("spark table目录" +dbname+"创建失败");
			}
			table.createTable();
		}	
	}
	
	public void register(String dbname,String tablename, String tabledefine, String path, boolean overwrite) throws SclibException
	{
		SparkTable table = new SparkTable(dbname,tablename, tabledefine,rootDir);
		try {
			if(path!=null && path.length()>0)
			{	
				JavaRDD<String> rdd = sc.textFile(FileUtil.getPath(path));
				Class cl = Class.forName(table.getTableClassName());
				JavaRDD<?> Trdd = DataSetLoader.toRdd(rdd,cl ,table.getFieldName());
				JavaSchemaRDD javaSch = stx.applySchema(Trdd, cl);
				stx.registerRDDAsTable(javaSch, tablename);
			}
		} catch (Exception e) {
			logger.error("registerRDDAsTable error:",e);
		}
		
		
		sparkTableMap.put(dbname+"." + tablename, table);
		logger.info("register table success:" + tablename);
	}
	
	public SparkTable getSparkTable(String tablename)
	{
		return getSparkTable("spark",tablename);
	}
	
	public SparkTable getSparkTable(String dbname,String tablename)
	{
		return sparkTableMap.get(dbname+"."+tablename);
	}
	
	//table存在标准：定义文件、class文件、jar文件都在
	private boolean isExistTable(String dbname, String tablename)
	{
		if(FileUtil.isExist(new String[]{rootDir,dbname}, tablename+".class") && FileUtil.isExist(new String[]{rootDir,dbname}, tablename+".jar"))
			return true;
		else
		{
			FileUtil.delFile(new String[]{rootDir,dbname}, tablename+".class");
			FileUtil.delFile(new String[]{rootDir,dbname}, tablename+".jar");
		}
		return false;
	}
	
	private boolean delTable(String dbname, String tablename) throws SclibException
	{
		if(FileUtil.delFile(new String[]{rootDir,dbname}, tablename+".class") && FileUtil.delFile(new String[]{rootDir,dbname}, tablename+".jar"))
			return true;
		return false;
	}
	

}

