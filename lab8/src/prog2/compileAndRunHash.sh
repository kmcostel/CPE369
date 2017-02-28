#!/bin/bash
javac -cp ../hadoop-core-1.2.1.jar hashtags.java 
jar cvf hashtags.jar *.class
hadoop jar hashtags.jar hashtags hashtags-input.json
