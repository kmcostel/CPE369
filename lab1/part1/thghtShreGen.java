/* Authors: Holly Haraguchi (hharaguc@calpoly.edu), Kevin Costello (kmcostel@calpoly.edu) */

import java.io.*;
import org.json.simple.*;

/* Data generation program */
public class thghtShreGen {
   
   /* Assumes that valid CML arguments will be given in the following order:
    * |numObjs| |outputFileName|
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
            System.out.println("Usage: thghtShreGen <numJsonObjects> <outputFileName>");
         }      
         JsonGen gen = new JsonGen(numObjs, outFile);

         try (FileWriter file = new FileWriter(outFile)) {
            for (int i = 0; i < numObjs; i++) {
               file.write(gen.JSON_Object().toJSONString() + "\n");
            } 
            file.close();
         }
         catch (Exception e) {
            System.out.println("Error opening '" + outFile + "': " + e.toString());
            System.out.println("Usage: thghtShreGen <numJsonObjects> <outputFileName>");
         }
      }      
      else {
         System.out.println("Usage: thghtShreGen <numJsonObjects> <outputFileName>");
      }
   }

}
