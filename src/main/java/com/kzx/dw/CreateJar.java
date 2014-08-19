package com.kzx.dw;

import java.util.List;
import org.apache.log4j.Logger;


import com.kzx.dw.bean.TableInfo;
import com.kzx.dw.util.TableStringUtil;

public class CreateJar {
	private static Logger logger = Logger.getLogger(CreateJar.class);
	public static void main(String[] args) {
		if(args.length<2)
		{
			System.out.println("usage: rootdir intables");
			return;
		}
		
		if(args!=null && args.length>0)
		{
			for(String s : args)
				logger.info("para:" +s);
		}
		
		String rootdir = args[0];
		
		String intables = args[1];			
		try {
			if(intables==null || intables.length()<1)
				throw new Exception("intables param is null");
			List<TableInfo> inList = TableStringUtil.getTableInfo(intables);
			SparkTableManager manager = new SparkTableManager();
			manager.init(rootdir);

			if(inList!=null)
			{
				for(TableInfo info : inList)
				{
					manager.createTableJar("spark", info.getTablename(), info.getTabledefine(), info.getPath(),true);
				}
			}
			
		} catch (Exception e) {
			logger.error("create jar error:" ,e);
		}
	}
}
