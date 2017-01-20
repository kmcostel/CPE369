##Lab 1 Part 2

###Contributors: 
* Holly Haraguchi (hharaguc@calpoly.edu)
* Kevin Costello (kmcostel@calpoly.edu)

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

General Assumptions: 
* When calculating the average rating of a movie, we include in the total number of ratings for each movie the number of zero ratings it received. This is to say the average is of all ratings, not just non-zero ratings.
* When calculating standard deviation of ratings, the ratings with a score of zero are also included in the standard deviation.
* When computing the average movie rating for each gender value, scores of zero contribute to the total rating count when computing the average.
