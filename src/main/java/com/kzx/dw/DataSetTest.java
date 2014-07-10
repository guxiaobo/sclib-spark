/**
* @Title: DataSetTest.java
* @Description: 测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:02:58 
 */
package com.kzx.dw;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.kzx.dw.bean.Person;
import com.kzx.dw.bean.SubPerson;

public class DataSetTest {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "DataSetTest");
		final JavaSQLContext stx = new JavaSQLContext(sc);
		JavaRDD<String> rdd = sc.textFile("person");

		final JavaRDD<Person> person = DataSetTransformer.rddStringToT(rdd, Person.class);
		
			
		ExecutorService executor = Executors.newFixedThreadPool(3);
		
		Future<JavaSchemaRDD> future = executor.submit(new Callable<JavaSchemaRDD>() {
			/* (non-Javadoc)
			 * @see java.util.concurrent.Callable#call()
			 */
			@Override
			public JavaSchemaRDD call() throws Exception {
				// TODO Auto-generated method stub
				return DataSetAnalyzer.max(stx,Person.class, person, "name","age");
			}
		});
				
		Future<JavaSchemaRDD> future1 = executor.submit(new Callable<JavaSchemaRDD>() {
			/* (non-Javadoc)
			 * @see java.util.concurrent.Callable#call()
			 */
			@Override
			public JavaSchemaRDD call() throws Exception {
				// TODO Auto-generated method stub
				return DataSetAnalyzer.groupByKey(stx,Person.class, person, "name","age");
			}
		});
		
		try {
			for(SubPerson sub : DataSetTransformer.schemaRDDToT(future1.get(), SubPerson.class).collect())
			{
				System.out.println("subname=" + sub.getName() + ",subage=" + sub.getAge());
			}
			
			for(SubPerson sub : DataSetTransformer.schemaRDDToT(future.get(), SubPerson.class).collect())
			{
				System.out.println("subname=" + sub.getName() + ",subage=" + sub.getAge());
			}

		} catch (Exception e) {
			// TODO: handle exception
		}
		executor.shutdown();
	}
	
}

