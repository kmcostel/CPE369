##Lab 1 Part 2

###Contributors: 
* Holly Haraguchi (hharaguc@calpoly.edu)
* Kevin Costello (kmcostel@calpoly.edu)

###Downloads
* .jar files: [json-simple-1.1.1.jar](https://code.google.com/archive/p/json-simple/downloads)

###Compilation Instructions:
    javac -cp json-simple-1.1.1.jar surveyGen.java SurveyJson.java
    java -cp json-simple-1.1.1.jar:. surveyGen <outputFileName> <numJsonObjects>

    javac -cp json-simple-1.1.1.jar surveyStats.java SurveyReader.java
    java -cp json-simple-1.1.1.jar:. surveyStats <jsonInputFileName> [outputFileName]

###Description of CML parameters
	JSON Generator parameters:
		<outputFileName>: The name of the file the generated JSON objects is to be written to.
		<numJsonObjects>: The number of JSON objects to be generated.

	JSON Statistics parameters:
		<jsonInputFileName>: The file that contains the JSON objects to be analyzed.    
		[outputFileName]: Optional parameter; the name of the file that a JSON object, representing 
                      the JSON statistics, is to be writren to.

###General Assumptions: 
* We interpreted non-zero ratings to mean any score for a movie that is not zero.
  Another possible way this might have been interpreted is for non zero ratings
  to mean ratings done by individuals who gave no zero ratings for any movie. 
* In our averages for movie ratings and standard deviations, we are only using
  data from individuals who have seen the movie (ie. they provided scores > 0)
 
