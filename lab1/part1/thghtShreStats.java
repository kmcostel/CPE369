
public class thghtShreStats {
  
   public static void main(String args[]) {
      /* Error handling in here.
       * Get file name and whether output will also go into a file.
       * Pass along JSON file name of input to the stat generator
       */
       String inFile;
       String outFile;
       
       if (args.length == 0) {
          /* Output a help message */
          System.out.println("We need more than that.");
       }
       else if (args.length == 1) {
       /* No output to a file */
          inFile = args[0];
          thghtShreStatCreater stats = new thghtShreStatCreater(inFile);
       }
       else if (args.length == 2) {
       /* Output to a file in addition to stdOut */
          inFile = args[0];
          outFile = args[1];
          thghtShreStatCreater stats = new thghtShreStatCreater(inFile, outFile);
       }
       else {
          System.out.println("Why so many arguments? Should we still take the first two?");
       }
       
      
   }
  
}