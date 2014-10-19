/**
* @Title: DataSetTransformer.java
* @Description: 数据集通用转化类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月7日 下午3:38:54 
 */
package com.kzx.dw;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class DataSetLoader implements Serializable{

	/**
	 *@Description: JavaRDD<String> 转成JavaRDD<Row>
	 *@param rdd
	 *@return
	 */
	public static JavaRDD<Row> toRow(JavaRDD<String> rdd)
	{
		return rdd.map(new Function<String, Row>() 
		{
			public Row call(String record) throws Exception {
				String[] fields = record.split("\\t");
				return Row.create(fields);
			}
		});
	}
	
	
	
	/**
	 *@Description: 通用JavaSchemaRDD类型RDD转成指定类型（T）RDD
	 *@param schema
	 *@return
	 */
	public static <T> JavaRDD schemaRDDToT(JavaSchemaRDD schemaRdd, final Class<T> cl, final List<String> fieldsName){
		
		System.out.println("test=" + cl.getSimpleName());
		
		return schemaRdd.map(new Function<Row, T>() {
			
			public T call(Row row) throws Exception {
				T t1= (T)cl.newInstance();						
				int i=0;
				for(String filedName :fieldsName)
				{
					Object filed = row.get(i);
					String setName = filedName.substring(0,1).toUpperCase() + filedName.substring(1); 
						
					Class<?> type = t1.getClass().getDeclaredField(filedName).getType();

					t1.getClass().getMethod("set"+setName,type).invoke(t1, filed);
					
					i++;
				}				
				return t1;
			}
		});				
	}
}

