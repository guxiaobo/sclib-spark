@echo off

set ROOT_DIR="d:\\sc"

set JAR_DIR="d:\\workspace\\sclib-spark\\target"

set MASTER="local[2]"

set SQL="select name,prov,age from person"

set INPUTTABLES="person||String name;int age;String prov||d:\\workspace\\sclib-spark\\person"

set OUTPUTPATH="d:\\test"


cmd /V /E /C java -classpath "%JAR_DIR%\sclib-spark-0.0.1-SNAPSHOT.jar"  com.kzx.dw.CreateJar %ROOT_DIR% %INPUTTABLES%	
cmd /V /E /C java -classpath "%ROOT_DIR%\spark\*;%JAR_DIR%\sclib-spark-0.0.1-SNAPSHOT.jar"  com.kzx.dw.DataSetApp %ROOT_DIR% %MASTER%  %SQL%  %INPUTTABLES% %OUTPUTPATH%		
