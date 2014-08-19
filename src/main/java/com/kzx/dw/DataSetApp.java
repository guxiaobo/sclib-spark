/**
* @Title: DataSetTest.java
* @Description: 测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:02:58 
 */
package com.kzx.dw;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.kzx.dw.bean.TableInfo;
import com.kzx.dw.util.TableStringUtil;


public class DataSetApp {
	private static Logger logger = Logger.getLogger(DataSetApp.class);
	public static void main(String[] args) {
		try {
			
//			java -classpath '/tmp/sc/spark/*:sclib-spark/target/sclib-spark-0.0.1-SNAPSHOT.jar'  
//			com.kzx.dw.DataSetTest /tmp/sc local[2] "person||String name;int age;String prov||/home/hdpusr/workspace/sclib-spark/person" 
//			"select name,prov,age from person"  "age||String name;String prov;int age||/tmp/ds"

			
			if(args.length<4)
			{
				System.out.println("usage: rootdir master  sql intables [outpath]");
				return;
			}
			
			if(args!=null && args.length>0)
			{
				for(String s : args)
					logger.info("para:" +s);
			}
			
			String rootdir = args[0];
			String master = args[1];
			String sql = args[2];
			
			
			String intables = args[3];			
			//subperson||String name;String prov;int age||/tmp/fs 格式
			if(intables==null || intables.length()<1)
				throw new Exception("intables param is null");
			List<TableInfo> inList = TableStringUtil.getTableInfo(intables);
			
			
			String outpath = null;
			if(args.length==5 && args[4].length()>0)
			{
				outpath = args[4];
			}

			SparkConf conf = new SparkConf();
			conf.setAppName("sparksql");
			conf.setMaster(master);  
			
			JavaSparkContext sc = new JavaSparkContext(conf);
			JavaSQLContext stx = new JavaSQLContext(sc);

			SparkTableManager manager = new SparkTableManager(sc, stx);
			manager.init(rootdir);

			if(inList!=null)
			{
				for(TableInfo info : inList)
				{
					manager.register("spark", info.getTablename(), info.getTabledefine(), info.getPath(),true);
				}
			}
			
			
			JavaSchemaRDD schema = DataSetAnalyzer.sql(stx,sql);

			
			//不配置输出路径，默认使用console输出
			if(outpath==null)
			{
				logger.info("output to console");
				for(String s : DataSetOutPut.outPutJavaSchemaRDDToConsole(schema))
				{
					logger.info(s);
				}
			}
			else 
			{
				logger.info("output to file");
				DataSetOutPut.outPutJavaSchemaRDDToLocal(schema,outpath);
			}
			
		} catch (Exception e) {
			logger.error("DataSetApp error:",e);
		}
	}
	
}

