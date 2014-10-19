/**
* @Title: SparkTable.java
* @Description: TODO(Spark中table类)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月7日 下午2:56:45 
 */
package com.kzx.dw;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;


public class SparkTable {
	
	protected static Logger logger = Logger.getLogger(SparkTable.class);

	private String tablename;
	private List<String> fieldName = new ArrayList<String>();
	private List<String> fieldType = new ArrayList<String>();
	private String tabledefine;
	private StructType schema;

	public SparkTable(String tablename, String tabledefine) 
	{		
		this.tablename = tablename;	
		this.tabledefine = tabledefine;
	}
	
	public void init() throws SclibException
	{
		Set<String> defineSet = new HashSet<String>();       
        for(String segment : tabledefine.split(";"))
        {	        	
        	String []s = segment.split(" ");
        	if(s.length==2)
        	{
        		if(defineSet.contains(s[1]))
        		{
        			throw new SclibException("SparkTable create error: hava same field");
        		}
        		defineSet.add(s[1]);
        		fieldName.add(s[1]);
        		fieldType.add(s[0]);
        	}	
        } 
        
        List<StructField> fields = new ArrayList<StructField>();
        for(int i=0; i<fieldName.size(); i++)
        {
        	if(fieldType.get(i).equalsIgnoreCase("string"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.StringType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("byte"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.ByteType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("short"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.ShortType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("int"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.IntegerType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("long"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.LongType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("float"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.FloatType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("double"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.DoubleType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("bigdecimal"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.DecimalType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("byte[]"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.BinaryType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("boolean"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.BooleanType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("timestamp"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i), DataType.TimestampType, true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("list"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i),DataType.createArrayType(DataType.StringType, true),true));
        	}
        	else if(fieldType.get(i).equalsIgnoreCase("Map"))
        	{
        		fields.add(DataType.createStructField(fieldName.get(i),DataType.createMapType(DataType.StringType, DataType.StringType, true),true));
        	}
        	
        }
        schema = DataType.createStructType(fields);
	}
	
	public String getTablename() {
		return tablename;
	}
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	public List<String> getFieldName() {
		return fieldName;
	}
	public void setFieldName(List<String> fieldName) {
		this.fieldName = fieldName;
	}
	public List<String> getFieldType() {
		return fieldType;
	}
	public void setFieldType(List<String> fieldType) {
		this.fieldType = fieldType;
	}

	public StructType getSchema() {
		return schema;
	}

	public void setSchema(StructType schema) {
		this.schema = schema;
	}
	
	
}

