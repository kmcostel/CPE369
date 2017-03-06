# Kevin Costello, Holly Haraguchi
#!/bin/bash

hdfs dfs -mkdir test
hdfs dfs -put *-input.json test/
hdfs dfs -put movies.csv test/

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:json-simple-1.1.1.jar

# Prog 1: Accounting
hadoop com.sun.tools.javac.Main accounting.java
jar cvf accounting.jar accounting*class
hadoop jar accounting.jar accounting -libjars json-simple-1.1.1.jar test/accounting-input.json test/accounting-output 

# Prog 2: Hashtags
hadoop com.sun.tools.javac.Main hashtags.java
jar cvf hashtags.jar hashtags*class
hadoop jar hashtags.jar hashtags -libjars json-simple-1.1.1.jar test/hashtags-input.json test/hashtags-output

# Prog 3: Favorite Movie
hadoop com.sun.tools.javac.Main favoriteMovie.java
jar cvf favoriteMovie.jar favoriteMovie*class
hadoop jar favoriteMovie.jar favoriteMovie -libjars json-simple-1.1.1.jar test/movies.csv test/favoriteMovie-input.json test/favoriteMovie-output

# Prog 4: User Similarity
hadoop com.sun.tools.javac.Main userSim.java
jar cvf userSim.jar userSim*class
hadoop jar userSim.jar userSim -libjars json-simple-1.1.1.jar test/userSim-input.json test/userSim-output

