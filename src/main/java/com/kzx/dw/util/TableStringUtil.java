/**
* @Title: TableStringUtil.java
* @Description: TODO(用一句话描述该文件做什么)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月18日 下午2:51:31 
 */
package com.kzx.dw.util;

import java.util.ArrayList;
import java.util.List;

import com.kzx.dw.bean.TableInfo;

public class TableStringUtil {
	
	public static List<TableInfo> getTableInfo(String tablestring)
	{
		List<TableInfo> list = new ArrayList<TableInfo>();
		String[] out =  tablestring.split("\\|\\|");
		if(out==null || out.length%3!=0)
		{
			return null;
		}

		for(int i=0; i<out.length; i+=3)
		{
			TableInfo table = new TableInfo();
			table.setTablename(out[i]);
			table.setTabledefine(out[i+1]);
			table.setPath(out[i+2]);
			list.add(table);
		}
		return list;
		
	}
}

