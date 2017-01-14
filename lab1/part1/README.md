##Students:
* Holly Haraguchi (hharaguc@calpoly.edu)
* Kevin Costello (kmcostel@calpoly.edu)

###.jar files: [json-simple-1.1.1.jar](https://code.google.com/archive/p/json-simple/downloads)

###Run and Compilation Instructions for JSON Generator
    javac -cp json-simple-1.1.1.jar JsonGen.java thghtShreGen.java
    java -cp json-simple-1.1.1.jar:. thghtShreGen yourOutFile.txt 12 

###Compilation and Run Instructions for JSON Statistics
    javac -cp json-simple-1.1.1.jar thghtShreStats.java thghtShreStatCreater.java
    java -cp json-simple-1.1.1.jar:. thghtShreStats  output.txt

###Description of CML parameters:

###Description of generation assumptions made:
* We assume that the only possible recipient values given in the input is one of the 4 following values : {subscribers, self, all, users}
* For the conditional histograms in TR6 part 1, we are only printing out the above 4 possible values as possible recipients. We are not printing out the frequency of received messages for every unique userID; we are clumping all the possible userIDs of recipients to be processed as a single statistic under the recipient value "users".
* We assume that the generator takes two arguments strictly in the example order. The argument being the name of file the user wishes to generate output to, and the second being the number of JSON objects to generate.
