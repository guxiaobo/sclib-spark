/**
* @Title: SparkTableManager.java
* @Description: TODO(Spark Table管理类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月7日 下午3:02:21 
 */
package com.kzx.dw;

import java.util.HashMap;
import java.util.Map;



public class SparkTableManager {
	
	private static Map<String, SparkTable> sparkTableMap = new HashMap<String,SparkTable>();
	
	public static void register(SparkTable table)
	{
		sparkTableMap.put(table.getDbname()+"."+table.getTablename(), table);
	}
	
	public static void unregister(SparkTable table)
	{
		sparkTableMap.remove(table.getDbname()+"." +table.getTablename());
	}
	
	
	

}

