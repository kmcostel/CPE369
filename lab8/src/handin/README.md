###Authors
* Holly Haraguchi
* Kevin Costello

###Notes on runme.sh
* Creates a directory called 'test' on hdfs
* Puts the input files into the newly created 'test' directory
* Runs all the programs
* Adds json-simple-1.1.1.jar to your HADOOP_CLASSPATH

###Notes on getResults.sh
* Copies each program's output part-r-00000 file from hdfs into the directory in which this script is ran
 and renames them to <programName>-output.json  

