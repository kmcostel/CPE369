import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.HashSet;

public class thghtShreStatCreater { /* I don't know what to call this */
   private int publicCnt; /* public message count for histogram */
   private int privateCnt;
   private int protectedCnt;
   private int responseCnt;
   private HashMap<String, Integer> recipients;
   private String outFile;
   
   private HashSet<String> users; /* to report total number of unique users */
   private int numMsgs; /* can be gotten from adding public + private + protected counts */
   private int numWords;
   private int numChars;
   
   private String inFileName;
   private String outFileName;
   
   public thghtShreStatCreater(String JSONFile) {
      inFileName = JSONFile;
      outFileName = null;
      users = new HashSet<String>(); 
      recipients = new HashMap();
      createStats();
   }
   public thghtShreStatCreater(String JSONFile, String outFile) {
      inFileName = JSONFile;
      outFileName = outFile;
      users = new HashSet<String>(); 
      recipients = new HashMap();
      createStats();
   }
   
   private void createStats() {      
      try{
         JSONParser parser = new JSONParser();
     
        /*JSONArray msgs = (JSONArray)(jsonObj.get("text"));
        JSONArray users = (JSONArray)( */
     
     /* lot of faith in the user input here */
         File f = new File(inFileName);
         Scanner scanner = new Scanner(f);
     
         while (scanner.hasNextLine()) {
            String jsonStr = scanner.nextLine();
        
        /* process the json string now... */
            Object obj = parser.parse(jsonStr);
            JSONObject jsonObj = (JSONObject)(obj);
        
        /* Get keys */
            String text = (String)jsonObj.get("text");
            String user = (String)jsonObj.get("user");
            String recipient = (String)jsonObj.get("recipient");
            String status = (String)jsonObj.get("status");
        
        /* Add user to set to keep track of uniqueness */
            users.add(user);
        
            numMsgs++;
            numChars += text.length(); /* assume white space counts */
            numWords += text.split(" ").length;
           
            processStatus(status);
            processUser(user);
            processRecipient(recipient);  
         }
      }
      catch (Exception e) {
         System.out.println(e.toString());
      }
     
      System.out.println("\nBasic stats:\n");
      System.out.println("Message Status Histogram: ");
      System.out.println("Total messages: " + numMsgs);
      System.out.println("Public: " + Integer.toString(publicCnt));
      System.out.println("Protected: " + Integer.toString(protectedCnt));
      System.out.println("Private: "+ Integer.toString(privateCnt) + "\n");
      
      System.out.println("Number of unique users: " + users.size());
      System.out.println("Average word count: " + (float)(numWords) / numMsgs);
      System.out.println("Average character count: " + (float)(numChars) / numMsgs);           
   }
   
   private void processStatus(String status) {
      if (status.compareTo("private") == 0) {
         privateCnt++;
      }
      else if (status.compareTo("public") == 0) {
         publicCnt++;
      }
      else if (status.compareTo("protected") == 0) {
         protectedCnt++;
      }
      else {
         /* Error message */
         System.out.println("Check this mesage status out: " + status);
         System.exit(1);
      }
   }
   
   private void processUser(String userId) {
      users.add(userId);
   }
   
   private void processRecipient(String recipient) {
      if (recipients.containsKey(recipient)) {
         recipients.put(recipient, recipients.get(recipient) + 1);
      }
      else {
         recipients.put(recipient, 1);
      }
   }


}
