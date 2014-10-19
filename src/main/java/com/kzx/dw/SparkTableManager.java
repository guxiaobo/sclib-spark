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
import org.apache.spark.sql.api.java.Row;

import com.kzx.dw.bean.TableInfo;



public class SparkTableManager {
	
	private  Map<String, SparkTable> sparkTableMap = new HashMap<String,SparkTable>();
	private static Logger logger = Logger.getLogger(SparkTableManager.class);
	private JavaSparkContext sc;
	private JavaSQLContext stx;


	public SparkTableManager(JavaSparkContext sc,JavaSQLContext stx)
	{
		this.sc = sc;
		this.stx= stx;
	}
	
	
	public void register(TableInfo info)
	{
		
		SparkTable table = new SparkTable(info.getTablename(), info.getTabledefine());
		try {	
			table.init();
			JavaRDD<String> rdd = sc.textFile(info.getPath());
			JavaRDD<Row> rowRDD = DataSetLoader.toRow(rdd);
			System.out.println("=--------------------------=" + rowRDD.count());
			JavaSchemaRDD peopleSchemaRDD = stx.applySchema(rowRDD, table.getSchema());
			peopleSchemaRDD.registerTempTable(info.getTablename());
		} catch (Exception e) {
			logger.error("registerRDDAsTable error:",e);
			return;
		}
		sparkTableMap.put(info.getTablename(), table);
		logger.info("register table success:" + info.getTablename());
	}
	
	public SparkTable getSparkTable(String tablename)
	{
		if(sparkTableMap!=null)
			return sparkTableMap.get(tablename);
		return null;
	}

}

