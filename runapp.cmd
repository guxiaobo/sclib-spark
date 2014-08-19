@echo off

rem 设置应用数据根目录
set ROOT_DIR="e:\\sc"

rem 设置sclib-spark-0.0.1-SNAPSHOT.jar包目录
set JAR_DIR="e:\\workspace\\sclib-spark\\target"

rem 设置运行模式
set MASTER="local[2]"

rem 设置运行的sql
set SQL="select name,prov,age from person"

rem 设置输入数据表 格式    表名||表定义||表对应的输入文件||表名||表定义||表对应的输入文件||
set INPUTTABLES="person||String name;int age;String prov||e:\\workspace\\sclib-spark\\person"

rem 设置输出数据表  表名||表定义||输出文件目录 （不设置就会直接输出结果到屏幕）
set OUTPUTPATH="e:\\test"


cmd /V /E /C java -classpath "%ROOT_DIR%\spark\*;%JAR_DIR%\sclib-spark-0.0.1-SNAPSHOT.jar"  com.kzx.dw.DataSetApp %ROOT_DIR% %MASTER%  %SQL%  %INPUTTABLES% %OUTPUTPATH%
