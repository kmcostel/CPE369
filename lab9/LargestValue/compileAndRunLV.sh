#!/bin/bash
hadoop com.sun.tools.javac.Main LargestValue.java
jar cvf LargestValue.jar LargestValue*class
hadoop jar LargestValue.jar LargestValue  LargestValue-output 
