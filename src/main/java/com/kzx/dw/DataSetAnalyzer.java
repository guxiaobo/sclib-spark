/**
* @Title: DataSetAnalyzer.java
* @Description: 数据集通用分析处理类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月7日 下午3:37:48 
 */
package com.kzx.dw;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class DataSetAnalyzer {
	private static Logger logger = Logger.getLogger(DataSetAnalyzer.class);
		
	public static JavaSchemaRDD sql(JavaSQLContext stx, String sql)
	{
		logger.debug(sql);
		return stx.sql(sql);
	}
	
}
