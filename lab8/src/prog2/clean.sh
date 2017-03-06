#!/bin/bash

rm *.class
hdfs dfs -rm -r hashtags-output.json/*
hdfs dfs -rmdir hashtags-output.json
