/**
* @Title: DataSetTest.java
* @Description: 测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:02:58 
 */
package com.kzx.dw;

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
		JavaSQLContext stx = new JavaSQLContext(sc);
		JavaRDD<String> rdd = sc.textFile("person");

		JavaRDD<Person> person = DataSetTransformer.rddStringToT(rdd, Person.class);
		
		for(Person p : person.collect())
		{
			System.out.println("name=" + p.getName() + ",age=" + p.getAge() + ",prov=" + p.getProv());
		}
		
		JavaSchemaRDD schema = DataSetAnalyzer.groupByKey(stx,Person.class, person, "name","age");
		
		for(SubPerson sub : DataSetTransformer.schemaRDDToT(schema, SubPerson.class).collect())
		{
			System.out.println("subname=" + sub.getName() + ",subage=" + sub.getAge());
		}
	}
	
}

