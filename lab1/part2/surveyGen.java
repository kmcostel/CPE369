/* 
 * Lab 1-2
 * Authors: Holly Haraguchi (hharaguc@calpoly.edu) and Kevin Costello (kmcostel@calpoly.edu)
 * CPE 369, Winter 2016
 */
import java.io.*;
import org.json.simple.*;

/* Data generation program */
public class surveyGen {
   
   /* Assumes that valid CML arguments will be given in the following order:
    * |outputFileName| |numObjs| 
    */
   public static void main (String args[]) throws FileNotFoundException, IOException {
      int numObjs = -1;

      /* Valid user input */
      if (args.length == 2) {
         String outFile = args[0];
         try {
            numObjs = Integer.parseInt(args[1]);
         }
         catch (Exception e) {
            System.out.println("ERROR: " + e.toString());
            printHelp();
         }      
         
         /* Initialize the SurveyJson object */
         SurveyJson gen = new SurveyJson();

         try (FileWriter file = new FileWriter(outFile)) {
            for (int i = 0; i < numObjs; i++) {
               file.write(gen.genObject().toJSONString() + "\n");
            } 
            file.close();
         }
         catch (Exception e) {
            System.out.println("Error opening '" + outFile + "': " + e.toString());
            printHelp();
         }
      }      
      else {
         printHelp();
      }
   }

   private static void printHelp() {
      String helpMsg = "Usage: surveyStats <jsonInputFileName> [jsonOutputFileName]";
      System.out.println(helpMsg);
   }

}
