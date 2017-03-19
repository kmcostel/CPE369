# Holly Haraguchi, Kevin Costello
# Lab 9, Problem 7: Power Days
# Compiles and runs our PowerDays job

javac -cp ../hadoop-core-1.2.1.jar PowerDays.java
jar cvf Power.jar PowerDays*class
rm PowerDays*class
hadoop jar Power.jar PowerDays
