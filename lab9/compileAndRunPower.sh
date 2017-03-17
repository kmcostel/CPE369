#!/bin/bash
hadoop com.sun.tools.javac.Main PowerDays.java
jar cvf Power.jar PowerDays*class
hadoop jar Power.jar PowerDays
