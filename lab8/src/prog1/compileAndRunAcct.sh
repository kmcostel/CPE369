#!/bin/bash
javac -cp ../hadoop-core-1.2.1.jar accounting.java
jar cvf accounting.jar *.class
hadoop jar accounting.jar accounting accounting-input.json
