/**
* @Title: DataSetTest.java
* @Description: 测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:02:58 
 */
package com.kzx.dw;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.tools.ant.taskdefs.Java;
import org.eclipse.jetty.server.session.JDBCSessionIdManager.DatabaseAdaptor;

import com.kzx.dw.util.JavaSourceUtil;

public class DataSetTest {
	public static void main(String[] args) {
		try {
			String tablename = "Person";	    	
	        String define = "String name;int age;String prov;";
			SparkTable Person = new SparkTable(tablename,define);
			tablename = "SubPerson";
			define = "String name;int type";
			SparkTable SubPerson = new SparkTable(tablename,define);

		
			SparkConf conf = new SparkConf();
			conf.setAppName("test");
			conf.setMaster("yarn-cluster");
			conf.setSparkHome("/usr/local/spark");

	
			JavaSparkContext sc = new JavaSparkContext(conf);

			final JavaSQLContext stx = new JavaSQLContext(sc);
			JavaRDD<String> personrdd = sc.textFile("/tmp/person");
			JavaRDD<?> persondata = DataSetLoader.toRdd(personrdd,  Class.forName(Person.getClassName()), Person.getFieldName());
		
			JavaRDD<String> subpersonrdd = sc.textFile("/tmp/subperson");
			JavaRDD<?> subpersondata = DataSetLoader.toRdd(subpersonrdd,  Class.forName(SubPerson.getClassName()), SubPerson.getFieldName());

			
			Class[] cl = {Class.forName(Person.getClassName()),Class.forName(SubPerson.getClassName())};
			JavaRDD[] r = {persondata,subpersondata};
			
			JavaSchemaRDD schema = DataSetAnalyzer.sql(stx, cl, r,"select a,b from (select t1.name as a,t1.age as age,t2.type as b from Person t1 join SubPerson t2 where t1.name=t2.name) t1");
			DataSetOutPut.outPutJavaSchemaRDDToHdfs(schema,cl[1],SubPerson.getFieldName());
			for(Object sub : DataSetLoader.schemaRDDToT(schema, Class.forName(SubPerson.getClassName()),SubPerson.getFieldName()).collect())
			{
				System.out.println("subname2222=" + sub);
			}
			
			
		} catch (Exception e) {
			System.out.println(e);
		}
		
		
//
////		final JavaRDD<com.kzx.dw.bean.Person> person = DataSetLoader.rddStringToT(rdd, Person.class);
////		
////			
////		ExecutorService executor = Executors.newFixedThreadPool(3);
////		
////		Future<JavaSchemaRDD> future = executor.submit(new Callable<JavaSchemaRDD>() {
////			/* (non-Javadoc)
////			 * @see java.util.concurrent.Callable#call()
////			 */
////			@Override
////			public JavaSchemaRDD call() throws Exception {
////				// TODO Auto-generated method stub
////				return DataSetAnalyzer.max(stx,Person.class, person, "name","age");
////			}
////		});
////				
////		Future<JavaSchemaRDD> future1 = executor.submit(new Callable<JavaSchemaRDD>() {
////			/* (non-Javadoc)
////			 * @see java.util.concurrent.Callable#call()
////			 */
////			@Override
////			public JavaSchemaRDD call() throws Exception {
////				// TODO Auto-generated method stub
////				return DataSetAnalyzer.groupByKey(stx,Person.class, person, "name","age");
////			}
////		});
////		
////		try {
////			for(SubPerson sub : DataSetLoader.schemaRDDToT(future1.get(), SubPerson.class).collect())
////			{
////				System.out.println("subname=" + sub.getName() + ",subage=" + sub.getAge());
////			}
//			
////			for(SubPerson sub : DataSetTransformer.schemaRDDToT(future.get(), SubPerson.class).collect())
////			{
////				System.out.println("subname=" + sub.getName() + ",subage=" + sub.getAge());
////			}
//
//		} catch (Exception e) {
//			// TODO: handle exception
//		}
//		executor.shutdown();
	}
	
}

