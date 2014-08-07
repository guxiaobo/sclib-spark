/**
* @Title: JavaSourceUtil.java
* @Description: TODO(JavaBean生成、编译、打包)
* @author tovin/xutaota2003@163.com 
* @date 2014年8月5日 下午2:16:48 
 */
package com.kzx.dw.util;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import javax.tools.JavaFileObject.Kind;
	 
public class JavaSourceUtil {
	 
	    private static final String GET = "get";
	    private static final String SET = "set";

	    private static String operateName(String name) {
	        String alias = name;
	        char c = name.charAt(0);
	        if (Character.isLowerCase(c)) {
	            alias = Character.toUpperCase(c) + name.substring(1);
	        }
	        return alias;
	    }
	 
	    public static boolean set(Object t, String field, Object value) {
	        if (t == null || field == null) {
	            return false;
	        }
	        try {
	            Class<?> c = t.getClass();
	            Class<?> type = null;
	            try {
	                Field f = c.getDeclaredField(field);
	                type = f.getType();
	            } catch (NoSuchFieldException e) {
	                Field f = ((Class<?>) c.getGenericSuperclass())
	                        .getDeclaredField(field);
	                type = f.getType();
	            }
	            Method m = c.getMethod(SET + operateName(field), type);
	            m.invoke(t, value);
	            return true;
	        } catch (SecurityException e) {
	            e.printStackTrace();
	        } catch (NoSuchMethodException e) {
	            e.printStackTrace();
	        } catch (IllegalArgumentException e) {
	            e.printStackTrace();
	        } catch (IllegalAccessException e) {
	            e.printStackTrace();
	        } catch (InvocationTargetException e) {
	            e.printStackTrace();
	        } catch (NoSuchFieldException e) {
	            e.printStackTrace();
	        }
	        return false;
	    }
	 
	    public static Object get(Object t, String field) {
	        if (t == null || field == null) {
	            return null;
	        }
	        try {
	            Method m = t.getClass().getMethod(GET + operateName(field));
	            return m.invoke(t);
	        } catch (SecurityException e) {
	            e.printStackTrace();
	        } catch (NoSuchMethodException e) {
	            e.printStackTrace();
	        } catch (IllegalArgumentException e) {
	            e.printStackTrace();
	        } catch (IllegalAccessException e) {
	            e.printStackTrace();
	        } catch (InvocationTargetException e) {
	            e.printStackTrace();
	        }
	        return null;
	    }
	 
	    public static Class<?> loadClass(Class<?> parent, String name,
	            final byte[] data) {
	        if (data == null) {
	            return null;
	        }
//	        parent.getClassLoader()
	        ClassLoader loader = new ClassLoader() {
	            @Override
	            protected Class<?> findClass(String name)
	                    throws ClassNotFoundException {
	            	System.out.println("findname=" + name);
	                return this.defineClass(name, data, 0, data.length);
	            }
	        };
	        try {
	            Class<?> nc = loader.loadClass("com.kzx.dw.bean."+ name);
	            return nc;
	        } catch (ClassNotFoundException e) {
	            e.printStackTrace();
	        }
	        return null;
	    }
	 
	    private static class SourceJavaFileObject extends SimpleJavaFileObject {
	 
	        private String source;
	 
	        protected SourceJavaFileObject(String name, String source) {
	            super(URI.create("string:///" + name.replace(".", "/")
	                    + Kind.SOURCE.extension), Kind.SOURCE);
	            this.source = source;
	        }
	 
	        @Override
	        public CharSequence getCharContent(boolean ignoreEncodingErrors)
	                throws IOException {
	            return source;
	        }
	    }
	 
	    private static class ClassJavaFileObject extends SimpleJavaFileObject {
	 
	        private ByteArrayOutputStream baos;
	 
	        protected ClassJavaFileObject(String name) {
	            super(URI.create("bytes:///" + name), Kind.CLASS);
	            this.baos = new ByteArrayOutputStream();
	        }
	 
	        @Override
	        public OutputStream openOutputStream() throws IOException {
	            return this.baos;
	        }
	 
	        public byte[] getBytes() {
	            return this.baos.toByteArray();
	        }
	    }
	 
	    private static byte[] compile(String name, String source) {
	        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
	        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
	        StandardJavaFileManager sfm = compiler.getStandardFileManager(
	                diagnostics, null, null);
	        final ClassJavaFileObject out = new ClassJavaFileObject(name);
	        JavaFileManager jfm = new ForwardingJavaFileManager<StandardJavaFileManager>(
	                sfm) {
	            @Override
	            public JavaFileObject getJavaFileForOutput(Location location,
	                    String className, Kind kind, FileObject sibling)
	                    throws IOException {
	                return out;
	            }
	        };
	        JavaCompiler.CompilationTask task = compiler.getTask(null, jfm, null,
	                null, null,
	                Arrays.asList(new SourceJavaFileObject(name, source)));
	        if (task.call()) {
	            return out.getBytes();
	        }
	        return null;
	    }
	 
	    
	    /* (non-Javadoc)
	    * @see java.lang.Object#toString()
	    */
	    @Override
	    public String toString() {
	    // TODO Auto-generated method stub
	    return super.toString();
	    }
	    public static String makeJavaSource(Map<String,String> tableDefine,String tablename) {
	     
	 
	        StringBuilder source = new StringBuilder();

	        source.append("package ").append("bean").append(";\n");
	 
	        // imports
	        source.append("import java.io.Serializable;\n");

	        // class name & extends
	        source.append("public class ").append(tablename).append(" implements Serializable ").append("\n{\n");
	 
	        // property 
	        for(Map.Entry<String, String> entry : tableDefine.entrySet())
	        {
	        	String name = entry.getKey();
	        	String type = entry.getValue();
	        	source.append("\tprivate ").append(type).append(" ").append(name).append(";\n");
	        }
	        
	        String s="";
	        int i=0;
	        // get&set method 
	        for(Map.Entry<String, String> entry : tableDefine.entrySet())
	        {
	        	String name = entry.getKey();
	        	String type = entry.getValue();
	        	//get
	        	source.append("\tpublic "+type+ " get");
	        	source.append(name.substring(0,1).toUpperCase()+ name.substring(1,name.length()));
	        	source.append("()\n\t{\n");
	        	source.append("\t\treturn " + name + ";\n");
	        	source.append("\t}\n\n");
	        	//set 
	        	source.append("\tpublic void set");
	        	source.append(name.substring(0,1).toUpperCase()+ name.substring(1,name.length()));
	        	source.append("("+type +" " + name +")\n\t{\n");
	        	source.append("\t\tthis." + name + "=" + name + ";\n");
	        	source.append("\t}\n\n");
	        	
	        	if(i>0)
	        		s+= "+\"\\t\"+";
	        	s+=name;	
	        	i++;
	        }
	        
	        //toString
	        source.append("\t @Override\n");
	        source.append("\t public String toString() {\n");
        	source.append("\t\treturn " + s + ";\n");
        	source.append("\t}\n\n");
	        source.append("}");
	        
	        return source.toString();
	    }
	 
	    private static String makeGetMothed(String name, Class<?>[] pts) {
	        StringBuilder method = new StringBuilder();
	        method.append("Method mhd_").append(name).append(" = getMethod(\"")
	                .append(name).append("\"");
	        for (Class<?> pt : pts) {
	            method.append(",").append(pt.getName()).append(".class");
	        }
	        method.append(");");
	        return method.toString();
	    }
	 
	    private static Object primitiveDefaultValue(Class<?> type) {
	        Object v = null;
	        if (type.isPrimitive()) {
	            if (type == boolean.class) {
	                v = false;
	            } else if (type == char.class) {
	                v = (char) 0;
	            } else if (type == byte.class) {
	                v = (byte) 0;
	            } else if (type == short.class) {
	                v = (short) 0;
	            } else if (type == int.class) {
	                v = 0;
	            } else if (type == long.class) {
	                v = 0L;
	            } else if (type == float.class) {
	                v = 0F;
	            } else if (type == double.class) {
	                v = 0D;
	            } else if (type == void.class) {
	                v = null;
	            }
	        }
	        return v;
	    }
	 
	    private static Class<?> primitive2Wrapper(Class<?> type) {
	        if (type.isPrimitive()) {
	            if (type == boolean.class) {
	                type = Boolean.class;
	            } else if (type == char.class) {
	                type = Character.class;
	            } else if (type == byte.class) {
	                type = Byte.class;
	            } else if (type == short.class) {
	                type = Short.class;
	            } else if (type == int.class) {
	                type = Integer.class;
	            } else if (type == long.class) {
	                type = Long.class;
	            } else if (type == float.class) {
	                type = Float.class;
	            } else if (type == double.class) {
	                type = Double.class;
	            } else if (type == void.class) {
	                type = Void.class;
	            }
	        }
	        return type;
	    }
	    
	    
	    public static String getJarPath(String define, String tablename)
	    {
	    	Map<String,String> tableDefine = new HashMap<String,String>();
	        for(String segment : define.split(";"))
	        {	        	
	        	String []s = segment.split(" ");
	        	if(s.length==2)
	        	{
	        		if(tableDefine.containsKey(s[1]))
	        			return null;
	        		tableDefine.put(s[1], s[0]);
	        	}	
	        	
	        }
	       
	        String source = makeJavaSource(tableDefine, tablename);
	      
	        byte[] data = compile(tablename, source);
	        
	        
		    try {
		    	String path = "/tmp/sclib/bean";
		    	File f=new File(path);
		    	if(!f.isDirectory())
		    	{
		    		mkDir(f);
		    	}
		    	
		    	String file = path+"/"+tablename+".class";
		    	
		    	f=new File(file);
		    	if(!f.exists())
		    	{
		    		f.createNewFile();
		    		System.out.println("文件"+f.getPath()+"已创建");
		    	}
		    	else
		    	{
		    		f.delete();
		    		f.createNewFile();
		    		System.out.println("文件"+f.getPath()+"已创建1111");
		    	}
		    		               
		    	FileOutputStream fos=new FileOutputStream(f);
		    	fos.write(data);
		    	fos.close();
		    	String targetJar = tablename + ".jar";
		    	CommandUtil.execute("rm /tmp/sclib/" + targetJar);
		    	System.out.println(CommandUtil.execute("cd /tmp/sclib;jar cvf " + targetJar  +" bean/" + tablename +".class"));
		    	
		    		
		    	 
		    	return targetJar;
			       
			} catch (Exception e) {
				LogUtil.UTILLOG.error("JavaSourceUtil#getJarPath error" ,e );
			}  
		    return null;
	    }
	    
	    //创建递归目录
	    private static void mkDir(File file){
	    	if(file.getParentFile().exists()){
	    		file.mkdir();
	    	}else{
	    		mkDir(file.getParentFile());
	    		file.mkdir();
	    	}
	    }
	    
	    public static Class<?>  getClass(String define, String tablename)
	    {
	    	
	        Map<String,String> tableDefine = new HashMap<String,String>();
	        for(String segment : define.split(";"))
	        {	        	
	        	String []s = segment.split(" ");
	        	if(s.length==2)
	        	{
	        		if(tableDefine.containsKey(s[1]))
	        			return null;
	        		tableDefine.put(s[1], s[0]);
	        	}	
	        	
	        }
	       
	        String source = makeJavaSource(tableDefine, tablename);
	      
	        byte[] data = compile(tablename, source);
	        	        
		    try {
		    	  Class<?> cl = loadClass(JavaSourceUtil.class, tablename, data);		    	 
		    	  return cl;
			       
			} catch (Exception e) {
				LogUtil.UTILLOG.error("JavaSourceUtil#getClass error" ,e );
			}  
		    return null;
	    }
	 
	    public static void main(String[] args) {
			String tablename = "Person";	    	
	        String define = "String name;int age;String prov;";
	        Map<String,String> tableDefine = new HashMap<String,String>();
	        for(String segment : define.split(";"))
	        {	        	
	        	String []s = segment.split(" ");
	        	if(s.length==2)
	        	{
	        		if(tableDefine.containsKey(s[1]))
	        			return ;
	        		tableDefine.put(s[1], s[0]);
	        	}	
	        	
	        }
	    	String source = makeJavaSource(tableDefine, tablename); 
	    	System.out.println(source);
	       
	    }

}

