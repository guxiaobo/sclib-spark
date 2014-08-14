/**
* @Title: DataSetTest.java
* @Description: 测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:02:58 
 */
package com.kzx.dw;

import java.net.URL;
import java.net.URLClassLoader;
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
			
			SparkTableManager manager = new SparkTableManager();
			manager.init("/tmp/sc");
			String tablename = "Person";	
			String tabledefine = "String name;int age;String prov;";
			manager.register(tablename, tabledefine,true);
			
			SparkConf conf = new SparkConf();
			conf.setAppName("test");
			conf.setMaster("local");
	
			conf.setJars(new String[]{manager.getSparkTable(tablename).getTableJarAbsolutePath()});
			
			
            
			JavaSparkContext sc = new JavaSparkContext(conf);

			
			URL url1 = new URL("file://"+manager.getSparkTable(tablename).getTableJarAbsolutePath());  
            URLClassLoader myClassLoader1 = new URLClassLoader(new URL[] { url1 }, Thread.currentThread()  
                    .getContextClassLoader());              
           Class cl = myClassLoader1.loadClass(manager.getSparkTable(tablename).getTableClassName());
			
           
           
			final JavaSQLContext stx = new JavaSQLContext(sc);
			JavaRDD<String> personrdd = sc.textFile("person");
			JavaRDD<?> persondata = DataSetLoader.toRdd(personrdd,cl , manager.getSparkTable(tablename).getFieldName());
			System.out.println("count=" + persondata.count());	
			System.out.println("name11111111111=" + cl.getName());
			
//			Class[] cls = {cl};
//			JavaRDD[] r = {persondata};
//			
//			JavaSchemaRDD schema = DataSetAnalyzer.sql(stx, cls, r,"select name,age,prov from Person");
//			System.out.println("name222222222222=" + cl.getName());
//			for(Object sub : DataSetLoader.schemaRDDToT(schema,cl ,manager.getSparkTable(tablename).getFieldName()).collect())
//			{
//				System.out.println("subname2222=" + sub);
//			}		
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

