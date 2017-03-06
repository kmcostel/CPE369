#!/bin/bash

hadoop com.sun.tools.javac.Main favoriteMovie.java

jar cvf favoriteMovie.jar favoriteMovie*class

hadoop jar favoriteMovie.jar favoriteMovie -libjars ../json-simple-1.1.1.jar movies.csv favoriteMovie-input.json test/final

