#!/bin/bash

javac -cp hadoop-core-1.2.1.jar:json-simple-1.1.1.jar hashtags.java 

jar cvf hashtags.jar *.class

hadoop jar hashtags.jar  hashtags lab8_inputs/hashtags-input.json lab8_outputs/prog2

