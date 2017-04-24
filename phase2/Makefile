# modify the paths appropriately
JDKPATH = /usr/lib/jvm/java-8-openjdk-amd64
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
