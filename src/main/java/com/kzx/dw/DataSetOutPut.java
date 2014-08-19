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

import com.kzx.dw.util.FileUtil;
import com.kzx.dw.util.OSUtil;


public class DataSetOutPut {
	
	public static <T> void  outPutJavaSchemaRDDToHdfs(JavaSchemaRDD schemaRdd, final Class<T> cl, final List<String> fieldsName, String outPutDir)
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
		}).saveAsTextFile(outPutDir);
	}
	
	public static <T> void  outPutJavaSchemaRDDToLocal(JavaSchemaRDD schemaRdd, String outPutDir)
	{
		JavaRDD<String> rdd = schemaRdd.map(new Function<Row, String>()
		{
			/* (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 */
			@Override
			public String call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				StringBuilder s = new StringBuilder();
				for(int i=0; i<v1.length(); i++)
				{
					s.append(v1.get(i)).append("\t");
				}
				return s.toString();
			}
		});
		
		if(OSUtil.isWin())
		{
			rdd.saveAsTextFile("file:\\\\\\" + outPutDir);
		}
		else
			rdd.saveAsTextFile("file://" + outPutDir);
		
	}
	
	public static List<String> outPutJavaSchemaRDDToConsole(JavaSchemaRDD schemaRdd)
	{
		return schemaRdd.map(new Function<Row, String>()
		{
			/* (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 */
			@Override
			public String call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				StringBuilder s = new StringBuilder();
				for(int i=0; i<v1.length(); i++)
				{
					s.append(v1.get(i)).append("\t");
				}
				return s.toString();
			}
		}).collect();
	}

}

