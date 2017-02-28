#!/bin/bash
javac -cp ../hadoop-core-1.2.1.jar favoriteMovie.java 
jar cvf favoriteMovie.jar *.class
hadoop jar favoriteMovie.jar favoriteMovie favoriteMovie-input.json
