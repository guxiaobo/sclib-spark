/**
* @Title: SubPerson.java
* @Description: java bean测试类
* @author tovin/xutaota2003@163.com 
* @date 2014年7月9日 上午11:02:37 
 */
package com.kzx.dw.bean;

import java.io.Serializable;

import com.kzx.dw.MetaAnnotation;

@MetaAnnotation(name={"name","age"})

public class SubPerson implements Serializable{
	private String name;
	private int age;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
}

