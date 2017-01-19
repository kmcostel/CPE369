/* Authors: Holly Haraguchi (hharaguc@calpoly.edu), Kevin Costello (kmcostel@calpoly.edu) */
import java.lang.Math;
import java.lang.System;
import java.util.*;
import org.json.simple.*;
import java.io.*;

public class JsonGen {

   private int msgId;
   private String user;
   private String msgStatus;
   private String recip;
   private String msgText;
   private int origMsg;
   private int numObjs;
   private List<String> wordList; // words to generate the message text from
   private String outFile;
   private Set<String> userIds;
   private static final int NUM_USERS = 10000;
   private static final String wordFile = "words/sense.txt";

   /* @fileName: name of the output file 
    * @numObjs: number of JSON objects to generate
    */
   public JsonGen (int numObjs, String fileName) throws FileNotFoundException {
      msgId = 0;
      this.numObjs = numObjs;
      this.outFile = fileName;
      userIds = new HashSet<String>();
      wordList = new ArrayList<String>();
      
      /* Insert message words into a list */
      try {
         Scanner s = new Scanner(new File(wordFile));
         while (s.hasNext()) {
            wordList.add(s.next());
         }
         s.close();
      }  
      catch (FileNotFoundException e) {
         System.out.println(wordFile + " not found\n");
      }
   }

   /* Returns an initialized JSONObject */
   public JSONObject JSON_Object() {
      Random random = new Random();
      JSONObject obj = new JSONObject();
      double rand = Math.random();
      
      /* Initialize |msgStatus| since genRecip depends on it */
      msgStatus = genStatus();
      user = genUserId();
      
      /* Add the unique ID to the set */
      userIds.add(user);
      
      /* Init the JSON object */      
      obj.put("messageId", genMsgId());
      obj.put("user", user);
      obj.put("status", msgStatus);
      obj.put("recipient", genRecipient());
      obj.put("text", genText());     
      
      /* The in-response message ID must be less than |msgId| */
      if (rand < 0.13) { 
         obj.put("in-response", random.nextInt(msgId));   
      }
      
      return obj;
   }
    
   /* Field generation methods */
   public String genMsgId() {
      return Integer.toString(msgId++);  
   }
   
   /* Generates a random, unique user ID */
   public String genUserId() {
      int id;    
      Random random = new Random();
      
      /* Generate a unique id */
      do {
         id = random.nextInt(NUM_USERS + 1);
      } while (userIds.contains(id));
      
      return "u" + Integer.toString(id);
   }
   
   /* Generates a random message status */
   private String genStatus() {
      double rand = Math.random();
      String status = "";
       
      /* Most messages are public */
      if (rand < 0.7) {
         status = "public"; // {any recipient value}
      }     
      else if (rand < 0.85) {
         status = "protected"; 
      }     
      else {
         status = "private"; 
      } 
      return status;
   }   
   
   /* Generates a valid, random recipient based on the message's status
    * Recipient can be "all", "self", "subscribers", or any single user id
    */
   private String genRecipient() {
       String recipient = "";
       double rand = Math.random();
       
       /* Public messages can be sent to all recipients */
       if (msgStatus.equals("public")) {
          if (rand < 0.45) {
             recipient = "subscribers";
          }
          else if (rand < 0.9) {
             recipient = "all";
          }
          else if (rand < 0.95) {
             recipient = "self";
          }
          else {
             recipient = genUserId();
          }
       }
       /* Private messages can be sent to self or a userID */
       else if (msgStatus.equals("private")) {
          if (rand < 0.8) { /* User id is recipient */
             recipient = genUserId();
          }
          else {
              recipient = "self";
          }
       }
       /* Protected messages can be sent to subscribers, self, or a userID */
       else if (msgStatus.equals("protected")) {
          /* Most protected messages are addressed to subscribers */
          if (rand < 0.7) {
             recipient = "subscribers";
          }
          else if (rand < 0.85) {
             recipient = "self";
          }
          else { /* < 1.0 */
             recipient = genUserId();
          }
       }
       else {
          System.out.println("ERROR: Message status '" + msgStatus + "' not recognized.");
          System.exit(1);
       }

       return recipient;
   }
   
   /* Generates a random string with length between 2 and 40 characters */
   private String genText() {
      String text = "";
      Random random = new Random();
      
      /* Length of text must be between 2 and 40 */
      int textLen = random.nextInt(39) + 2;
      int randIdx = random.nextInt(wordList.size());
      
      for (int i = 0; i < textLen; i++) {
         text += wordList.get(randIdx) + " ";
         randIdx = random.nextInt(wordList.size());
      }
      
      return text;
   }

}
