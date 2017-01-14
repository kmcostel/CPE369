##Students:
* Holly Haraguchi (hharaguc@calpoly.edu)
* Kevin Costello (kmcostel@calpoly.edu)

###.jar files: [json-simple-1.1.1.jar](https://code.google.com/archive/p/json-simple/downloads)

###Run and Compilation Instructions for JSON Generator
    javac -cp json-simple-1.1.1.jar JsonGen.java thghtShreGen.java
    java -cp json-simple-1.1.1.jar:. thghtShreGen <outputFileName> <numJsonObjects> 

###Compilation and Run Instructions for JSON Statistics
    javac -cp json-simple-1.1.1.jar thghtShreStats.java thghtShreStatCreater.java
    java -cp json-simple-1.1.1.jar:. thghtShreStats  <jsonInputFileName> [outputFileName]

###Description of CML parameters:
	JSON Generator parameters:
		<outputFileName>: The name of the file the generated JSON objects is to be written to.
		<numJsonObjects>: The number of JSON objects to be generated.
	JSON Statistics parameters:
		<jsonInputFileName>: The file that contains the JSON objects to be analyzed.
		[outputFileName]: Optional parameter; the name of the file that a JSON object, representing the JSON statistics, is to be written to.

###Description of generation assumptions made:
* We assume that the only possible recipient values given in the input is one of the 4 following values : {subscribers, self, all, user IDs}
* For the conditional histograms in TR6 part 1, we only print out the above 4 values as possible recipients. We are not printing out the frequency of received messages for every unique userID; we are clumping all the possible userIDs of recipients to be processed as a single statistic under the recipient value "users".
* We assume that the generator takes two arguments strictly in the following order: the first argument is the name of the output file, and the second is the number of JSON objects to generate.
