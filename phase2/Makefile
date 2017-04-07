# modify the paths appropriately
JDKPATH = /Library/Java/JavaVirtualMachines/jdk1.8.0_112.jdk/Contents/Home
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test Graph
GraphTest:GraphTest.java
	$(JAVAC) GraphTest.java

graphtest: GraphTest
	$(JAVA) phase2.GraphTest
