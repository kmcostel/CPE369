# Holly Haraguchi, Kevin Costello
# Lab 9, Problem 8 - Kitchen Use 
# Compiles and runs the KitchenUse job

javac -cp hadoop-core-1.2.1.jar KitchenUse.java
jar cvf Kitchen.jar KitchenUse*class
rm KitchenUse*class
hadoop jar Kitchen.jar KitchenUse kitchen-output 
