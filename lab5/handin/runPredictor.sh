# Holly Haraguchi and Kevin Costello
# Lab 5
# CPE 369, Winter 2017
# Runs ratingsPredictor and uses the first CML argument as the program's JSON input file

name=$1
java -cp json-simple-1.1.1.jar:mongo-java-driver-3.4.2.jar:. ratingsPredictor $name 
