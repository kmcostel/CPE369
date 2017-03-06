#!/bin/bash

javac -cp ../hadoop-core-1.2.1.jar:../json-simple-1.1.1.jar userSim.java 
jar cvf userSim.jar *.class
hadoop jar userSim.jar userSim -libjars ../json-simple-1.1.1.jar userSim-input.json userSim-output
