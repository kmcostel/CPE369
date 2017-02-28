#!/bin/bash
# Kevin Costello

hadoop com.sun.tools.javac.Main  hashtags.java

jar cvf myJob.jar *.class

hadoop jar myJob.jar  hashtags lab8_inputs/hashtags-input.json lab8_outputs/prog2
