/**
* @Title: DataSetAnalyzer.java
* @Description: 数据集通用分析处理类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月7日 下午3:37:48 
 */
package com.kzx.dw;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class DataSetAnalyzer {
	
	/**
	 *@Description: groupByKey
	 *@param stx
	 *@param cl
	 *@param rdd
	 *@param columnName
	 *@return
	 */
	public static <T> JavaSchemaRDD groupByKey(JavaSQLContext stx,final Class<T> cl, JavaRDD<T> rdd, String... columnName)
	{
		JavaSchemaRDD javaSch = 
				stx.applySchema(rdd, cl);
		String tablename = cl.getName();
		stx.registerRDDAsTable(javaSch, tablename);
		
		String colStr="";
		int i=0;
		for(String col : columnName)
		{
			if(i>0)
				colStr+= "," + col;
			else
				colStr+= col;
			i++;
		}
		
		JavaSchemaRDD result = stx.sql("select "+colStr+" from "+tablename+" group by " +colStr);
		return result;
	}
}
