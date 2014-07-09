/**
* @Title: MetaAnnotation.java
* @Description: 表字段标注
* @author tovin/xutaota2003@163.com 
* @date 2014年7月8日 下午4:36:55 
 */
package com.kzx.dw;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)

public @interface MetaAnnotation {
	String[] name() default "";
}
