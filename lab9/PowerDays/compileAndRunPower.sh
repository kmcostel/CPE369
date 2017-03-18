# Holly Haraguchi, Kevin Costello
# Lab 9, Problem 7: Power Days
# Compiles and runs our PowerDays job
hadoop com.sun.tools.javac.Main PowerDays.java
jar cvf Power.jar PowerDays*class
hadoop jar Power.jar PowerDays
