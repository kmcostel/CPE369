# Holly Haraguchi, Kevin Costello
# Lab 9, Problem 2: LargestValue 
# Compiles and runs the LargestValue job

javac -cp hadoop-core-1.2.1.jar LargestValue.java
jar cvf LargestValue.jar LargestValue*class
hadoop jar LargestValue.jar LargestValue  LargestValue-output 
