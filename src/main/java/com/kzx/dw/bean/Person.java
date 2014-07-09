/**
* @Title: Person.java
* @Description: java bean测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午12:03:50 
 */
package com.kzx.dw.bean;

import java.io.Serializable;

import com.kzx.dw.MetaAnnotation;

@MetaAnnotation(name={"name","age","prov"})
public class Person implements Serializable{
	private int age;
	private String name;
	private String prov;
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getProv() {
		return prov;
	}
	public void setProv(String prov) {
		this.prov = prov;
	}
}

