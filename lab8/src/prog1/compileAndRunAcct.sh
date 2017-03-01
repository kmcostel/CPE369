#!/bin/bash
hadoop com.sun.tools.javac.Main accounting.java
jar cvf accounting.jar accounting*class
hadoop jar accounting.jar accounting -libjars ../json-simple-1.1.1.jar accounting-input.json accounting-output 


