import java.io.*;
import org.json.simple.*;

/* Data generation program */
public class thghtShreGen {
   public static void main (String args[]) throws FileNotFoundException, IOException {
      /* Assume perfect user input for now */
      
      if (args.length == 2) {
         String outFile = args[0];
         int numObjs = Integer.parseInt(args[1]);
      }
      
      /* possible option param (as mentioned in the spec): word file for messages */
      JsonGen gen = new JsonGen(numObjs, outFile);

      try (FileWriter file = new FileWriter(outFile)) {
         for (int i = 0; i < numObjs; i++) {
            file.write(gen.JSON_Object().toJSONString() + "\n");
         } 
         file.close();
      }
   }
}
