## Lab 5 - Mongo Connectivity With Java

### Compilation Instructions
    ./compilePredictor.sh

### Run Instructions
    ./runPredictor.sh <JSON_File>

### Contributors
* Holly Haraguchi (hharaguc@calpoly.edu)
* Kevin Costello (kmcostel@calpoly.edu)

### Notes
* Our program assumes that the .json source file used to populate the ratings survey lists the objects line by line.
* After computing simililarities, we look for the existence of a collection named "similarities" and delete it if it exists. Then we recreate the collection and populate it with our similarity calculations.
* We assume the input.json file contains respondent IDs and movie IDs that exist in our database. There is no error checking for illegitimate values.
