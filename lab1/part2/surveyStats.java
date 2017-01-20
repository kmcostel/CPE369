/* 
 * Lab 1-2
 * Authors: Holly Haraguchi (hharaguc@calpoly.edu) and Kevin Costello (kmcostel@calpoly.edu)
 * CPE 369, Winter 2016
 */

public class surveyStats {
    public static void main(String args[]) {
        /* Get file name and whether output will also go into a file.
         * Pass along JSON file name of input to the stat generator
         */
        String inFile;
        String outFile;
              
        if (args.length == 0) {
            /* Output a help message */
            printHelp();
            System.exit(1);
        }
        else if (args.length == 1) {
            /* No output to a file */
            inFile = args[0];
            SurveyReader stats = new SurveyReader(inFile);
        }
        else if (args.length == 2) {
        /* Output to a file in addition to stdOut */
            inFile = args[0];
            outFile = args[1];
            SurveyReader stats = new SurveyReader(inFile, outFile);
        }
        else {
            printHelp();
            System.exit(1);
        }
    }
   
    private static void printHelp() {
        String helpMsg = "Usage: surveyStats <jsonInputFileName> [jsonOutputFileName]";
        System.out.println(helpMsg);
    }
  
}