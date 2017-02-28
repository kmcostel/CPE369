#!/bin/bash
javac -cp ../hadoop-core-1.2.1.jar userSim.java 
jar cvf userSim.jar *.class
hadoop jar userSim.jar userSim userSim-input.json
