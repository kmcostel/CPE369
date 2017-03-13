#!/bin/bash
hadoop com.sun.tools.javac.Main KitchenUse.java
jar cvf Kitchen.jar KitchenUse*class
hadoop jar Kitchen.jar KitchenUse kitchen-output 
