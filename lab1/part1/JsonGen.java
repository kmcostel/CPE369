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
      obj.put("recepient", genRecipient());
      obj.put("text", genText());     
      
      if (rand < 0.13) { /* the in-response message id needs to be less than msgId */
         obj.put("in-response", random.nextInt(msgId));   
      }
      
      return obj;
   }
    
   /* Field generation methods */
   public String genMsgId() {
      return Integer.toString(msgId++);  
   }
   
   public String genUserId() {
      int id;    
      Random random = new Random();
      
      /* Generate a unique id */
      do {
         id = random.nextInt(NUM_USERS + 1);
      } while (userIds.contains(id));
      
      return "u" + Integer.toString(id);
   }
   
   private String genStatus() {
      double rand = Math.random();
      String status = "";
       
      if (rand < 0.5) {
         status = "public"; // {any recipient value}
      }
      
      /* Most protected messages are addressed to "subscribers" */
      /* {self, subscribers, or userID} as possible recipients */
      else if (rand < 0.75) {
         status = "protected"; 
      }
      
      /* Private messages can have "self" or "userId" as recipient */
      else {
         status = "private"; 
      } 
      return status;
   }   
   
   /* Recipient is either "all", "self", "subscribers", or any single user id */
   private String genRecipient() {
       String recipient = "";
       double rand = Math.random();
       
       if (msgStatus.compareTo("public") == 0) {
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
       /* Private can either have self or a userID as a recipient */
       else if (msgStatus.compareTo("private") == 0) {
          if (rand < 0.8) { /* User id is recipient */
             recipient = genUserId();
          }
          else {
              recipient = "self";
          }
       }
       else if (msgStatus.compareTo("protected") == 0) {
          if (rand < 0.6) {
             recipient = "subscribers";
          }
          else if (rand < 0.7) {
             recipient = "self";
          }
          else if (rand < 0.8) {
             recipient = "all";
          }
          else { /* < 1.0 */
             recipient = genUserId();
          }
       }
       else {
          recipient = "How did you get here dum dum?";
       }

       return recipient;
   }
   
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
