/**
* @Title: DataSetOutPut.java
* @Description: TODO(DataSet数据输出类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月8日 上午11:29:12 
 */
package com.kzx.dw;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;


public class DataSetOutPut {
	
	public static <T> void  outPutJavaSchemaRDDToHdfs(JavaSchemaRDD schemaRdd, final Class<T> cl, final List<String> fieldsName)
	{
		schemaRdd.map(new Function<Row, T>() {
			/* (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 */
			@Override
			public T call(Row row) throws Exception {
				T t1= (T)cl.newInstance();		
				int i=0;
				for(String filedName : fieldsName)
				{
					Object filed = row.get(i);
					String setName = filedName.substring(0,1).toUpperCase() + filedName.substring(1); 
					Class<?> type = t1.getClass().getDeclaredField(filedName).getType();
					t1.getClass().getMethod("set"+setName,type).invoke(t1, filed);
					i++;
				}				
				return t1;
			}
		}).saveAsTextFile("/tmp/" + cl.getSimpleName());
	}
	
	public static <T> void outPutJavaRDDToHdfs(JavaRDD<T> rdd, final Class<T> cl)
	{
		rdd.saveAsTextFile("/tmp/" + cl.getSimpleName());
	}

}

