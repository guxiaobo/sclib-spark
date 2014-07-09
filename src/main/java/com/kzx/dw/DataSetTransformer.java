/**
* @Title: DataSetTransformer.java
* @Description: 数据集通用转化类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月7日 下午3:38:54 
 */
package com.kzx.dw;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class DataSetTransformer implements Serializable{

	/**
	 *@Description: 通用String类型RDD转成指定类型（T）RDD
	 *@param rdd
	 *@return
	 */
	public static <T> JavaRDD<T> rddStringToT(JavaRDD<String> rdd, final Class<T> cl){
		return rdd.map(new Function<String, T>() {
			/* (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 */
			@Override
			public T call(String v1) throws Exception {
				T t1= (T)cl.newInstance();		
				String[] fileds =v1.split("\\t");
				
				int i=0;
				for(String filedName :cl.getAnnotation(MetaAnnotation.class).name())
				{
					String filed = fileds[i];
					String setName = filedName.substring(0,1).toUpperCase() + filedName.substring(1); 						
					Class<?> type = t1.getClass().getDeclaredField(filedName).getType();						
					if(type.getName().equals("java.lang.String"))
						t1.getClass().getMethod("set"+setName,type).invoke(t1, filed);
					else if(type.getName().equals("int"))				
						t1.getClass().getMethod("set"+setName,type).invoke(t1, Integer.parseInt(filed));
					else if(type.getName().equals("float"))				
						t1.getClass().getMethod("set"+setName,type).invoke(t1, Float.parseFloat(filed));
					else if(type.getName().equals("double"))				
						t1.getClass().getMethod("set"+setName,type).invoke(t1, Double.parseDouble(filed));
					i++;
				}				
				return t1;
			}
		});				
	}
	
	/**
	 *@Description: 通用JavaSchemaRDD类型RDD转成指定类型（T）RDD
	 *@param schema
	 *@return
	 */
	public static <T> JavaRDD<T> schemaRDDToT(JavaSchemaRDD schemaRdd, final Class<T> cl){
		return schemaRdd.map(new Function<Row, T>() {
			/* (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 */
			@Override
			public T call(Row row) throws Exception {
				T t1= (T)cl.newInstance();						
				int i=0;
				for(String filedName :cl.getAnnotation(MetaAnnotation.class).name())
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

