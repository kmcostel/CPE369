/* Authors: Holly Haraguchi (hharaguc@calpoly.edu), Kevin Costello (kmcostel@calpoly.edu) */

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.HashSet;
import java.util.ArrayList;

public class thghtShreStatCreater {
   /* All messages */
   private HashMap<String, Integer> recipients;
   private HashSet<String> users; /* to report total number of unique users */
   
   private int numMsgs;
   private int numWords;
   private int numChars;
   private int inRespCnt; /* count for messages that are in-response to another message */
   
   private ArrayList<Integer> wordCounts;
   private ArrayList<Integer> charCounts;
   
   /* TR5.3 */
   private ArrayList<Integer> responseWords;
   private ArrayList<Integer> responseChars;
   private ArrayList<Integer> noResponseWords;
   private ArrayList<Integer> noResponseChars;
   
   private double avgWordCnt; /* average word count for all messages */
   private double stdDevWordCnt; /* standard deviation of average word count */
   
   private double avgCharCnt; /* average character count for all messages */
   private double stdDevCharCnt; /* standard deviation of average character count */
   
   /* Public messages */
   private int publicCnt; /* counter for public messages */
   
   private double avgWordCntPub;
   private double avgCharCntPub;
   
   private int numPubWords;
   private int numPubChars;
   
   private double stdDevWordPub;
   private double stdDevCharPub;
   
   /* Public recipient counts */
   private int pubSub;
   private int pubSelf;
   private int pubUser;
   private int pubAll;
   
   /* Number of public messages that are a response */
   private int pubResp;
   
   /* To be used for std of public messages */
   private ArrayList<Integer> pubWords;
   private ArrayList<Integer> pubChars;
    
   /* Protected messages */
   private int protectedCnt; /* counter for protected messages */
   
   private double avgWordCntProt;
   private double avgCharCntProt;
   
   private int numProtWords;
   private int numProtChars;
   
   private double stdDevWordProt;
   private double stdDevCharProt;
   
   /* Protected recipient counts */
   private int protSub;
   private int protSelf;
   private int protUser;
   private int protAll;
   
   /* Number of protected messages that are a response */
   private int protResp;
   
   /* To be used for std of protected messages */
   private ArrayList<Integer> protWords;
   private ArrayList<Integer> protChars;
   
   /* Private messages */
   private int privateCnt; /* counter for private messages */ 
   
   private double avgWordCntPriv;
   private double avgCharCntPriv;
   
   private int numPrivWords;
   private int numPrivChars;
   
   private double stdDevWordPriv;
   private double stdDevCharPriv;
   
   /* Private recipient counts */
   private int privSub;
   private int privSelf;
   private int privUser;
   private int privAll;
   
   /* Number of private messages that are a response */
   private int privResp;
   
   /* To be used for std of private messages */
   private ArrayList<Integer> privWords;
   private ArrayList<Integer> privChars;
   
   /* Recipient types = {subscribers, self, userId, all} */
   private ArrayList<Integer> subWordCount;
   private ArrayList<Integer> subCharCount;
   private double avgSubWords;
   private double avgSubChars;
   private double subWordStdDev;
   private double subCharStdDev;
   
   private ArrayList<Integer> selfWordCount;
   private ArrayList<Integer> selfCharCount;
   private double avgSelfWords;
   private double avgSelfChars;
   private double selfWordStdDev;
   private double selfCharStdDev;
   
   private ArrayList<Integer> userWordCount;
   private ArrayList<Integer> userCharCount;
   private double avgUserWords;
   private double avgUserChars;
   private double userWordStdDev;
   private double userCharStdDev;
   
   private ArrayList<Integer> allWordCount;
   private ArrayList<Integer> allCharCount;
   private double avgAllWords;
   private double avgAllChars;
   private double allWordStdDev;
   private double allCharStdDev;
   
   /* Track the number of in-response flags for each recipient value */
   private int selfRespCnt;
   private int subRespCnt;
   private int allRespCnt;
   private int userRespCnt;
        
   /* File IO */
   private String inFileName;
   private String outFileName;
   
   /* Key is number of words in message, value is a count */
   private HashMap<Integer, Integer> msgWordMap; 
   
   
   /* Constructor without specified output file -- just print to console */
   public thghtShreStatCreater(String JSONFile) {
      inFileName = JSONFile;
      outFileName = new String();
      initLists();
      initMsgWordMap();
      createStats();
   }
   
   /* Constructor with specified output file -- print to console and write JSON object to |outFile| */
   public thghtShreStatCreater(String JSONFile, String outFile) {
      inFileName = JSONFile;
      outFileName = outFile;
      initLists();
      initMsgWordMap();
      createStats();
   }
   
   private void initLists() {
      users = new HashSet<String>(); 
      recipients = new HashMap();
      wordCounts = new ArrayList<Integer>();
      charCounts = new ArrayList<Integer>();
      msgWordMap = new HashMap<Integer, Integer>();
      pubWords = new ArrayList<Integer>();
      pubChars = new ArrayList<Integer>();
      protWords = new ArrayList<Integer>();
      protChars = new ArrayList<Integer>();
      privWords = new ArrayList<Integer>();
      privChars = new ArrayList<Integer>();
      subWordCount = new ArrayList<Integer>();
      subCharCount = new ArrayList<Integer>();
      selfWordCount = new ArrayList<Integer>();
      selfCharCount = new ArrayList<Integer>();
      allWordCount = new ArrayList<Integer>();
      allCharCount = new ArrayList<Integer>();
      userWordCount = new ArrayList<Integer>();
      userCharCount = new ArrayList<Integer>();
      responseWords = new ArrayList<Integer>();
      responseChars = new ArrayList<Integer>();
      noResponseWords = new ArrayList<Integer>();
      noResponseChars = new ArrayList<Integer>();
   }
   
   private void createStats() {      
      try {
         JSONParser parser = new JSONParser();
     
         /* lot of faith in the user input here */
         File f = new File(inFileName);
         Scanner scanner = new Scanner(f);
     
         while (scanner.hasNextLine()) {
            String jsonStr = scanner.nextLine();
        
            /* Process the JSON string now... */
            Object obj = parser.parse(jsonStr);
            JSONObject jsonObj = (JSONObject)(obj);
        
            /* Get keys */
            String text = (String)jsonObj.get("text");
            String user = (String)jsonObj.get("user");
            String recipient = (String)jsonObj.get("recipient");
            String status = (String)jsonObj.get("status");
            
            /* Check if message is in-response */
            boolean inRespFlag = false;
            if (jsonObj.containsKey("in-response")) {
               inRespCnt++;
               inRespFlag = true;
            }
                  
            numMsgs++;
            
            int numChars = text.length();
            int wordCnt = text.split(" ").length;           
            
            numChars += numChars; /* Assume spaces count as characters */
            numWords += wordCnt;
            
            /* Assumption made that words is a value between 2 and 40 */
            msgWordMap.put(wordCnt, msgWordMap.get(wordCnt) + 1); /* Increment count */
                        
            charCounts.add(numChars);
            wordCounts.add(wordCnt);           
           
            processStatus(status, recipient, numChars, wordCnt, inRespFlag);
            processUser(user); /* Add user to set to keep track of uniqueness */
            /* ProcessRecipient also handles TR5.3 (Tracking message counts of 
               in response and not in response */
            processRecipient(recipient, wordCnt, numChars, inRespFlag);  
         }
      }
      catch (Exception e) {
         System.out.println(e.toString());
         System.exit(1);
      }
      
      /* Calculate averages and standard deviation for all messages */
      avgWordCnt = (double)numWords / numMsgs;
      avgCharCnt = (double)numChars / numMsgs;
      
      stdDevWordCnt = calcStdDev(wordCounts, avgWordCnt);     
      stdDevCharCnt = calcStdDev(charCounts, avgCharCnt);
      
      /* Calculate averages and standard deviation for public messages */
      avgWordCntPub = (double)numPubWords / publicCnt;
      avgCharCntPub = (double)numPubChars / publicCnt;
      
      stdDevWordPub = calcStdDev(pubWords, avgWordCntPub);
      stdDevCharPub = calcStdDev(pubChars, avgCharCntPub);
      
      /* Calculate averages and standard deviation for protected messages */
      avgWordCntProt = (double)numProtWords / protectedCnt;
      avgCharCntProt = (double)numProtChars / protectedCnt;
      
      stdDevWordProt = calcStdDev(protWords, avgWordCntProt);
      stdDevCharProt = calcStdDev(protChars, avgCharCntProt);
      
      /* Calculate averages and standard deviation for private messages */
      avgWordCntPriv = (double)numPrivWords / privateCnt;
      avgCharCntPriv = (double)numPrivChars / privateCnt;
     
      stdDevWordPriv = calcStdDev(privWords, avgWordCntPriv);
      stdDevCharPriv = calcStdDev(privChars, avgCharCntPriv);
      
      /* Averages for recipients */
      avgSubWords = calcAvg(subWordCount);
      avgSubChars = calcAvg(subCharCount); 
      subWordStdDev = calcStdDev(subWordCount, avgSubWords);
      subCharStdDev = calcStdDev(subCharCount, avgSubChars);
          
      avgSelfWords = calcAvg(selfWordCount);
      avgSelfChars = calcAvg(selfCharCount);
      selfWordStdDev = calcStdDev(selfWordCount, avgSelfWords);
      selfCharStdDev = calcStdDev(selfCharCount, avgSelfChars);
          
      avgAllWords = calcAvg(allWordCount);
      avgAllChars = calcAvg(allCharCount);
      allWordStdDev = calcStdDev(allWordCount, avgAllWords);
      allCharStdDev = calcStdDev(allCharCount, avgAllChars);
      
      avgUserWords = calcAvg(userWordCount);
      avgUserChars = calcAvg(userCharCount);
      userWordStdDev = calcStdDev(userWordCount, avgUserWords);
      userCharStdDev = calcStdDev(userCharCount, avgUserChars);
      
      
      /* Output */
      /* Basic stats */
      System.out.println("Basic stats:\n");
      System.out.println("   - Total messages: " + numMsgs);
      System.out.println("   - Total unique users: " + users.size());
      System.out.println("   - Average word count: " + avgWordCnt);
      System.out.println("   - Standard deviation for average word count: " + stdDevWordCnt);
      System.out.println("   - Average character count: " + avgCharCnt);     
      System.out.println("   - Standard deviation for average character count: " + stdDevCharCnt);
      
      /* Message type histogram */
      System.out.println("Message Type Histogram: ");    
      System.out.println("   - Public: " + publicCnt);
      System.out.println("   - Protected: " + protectedCnt);
      System.out.println("   - Private: " + privateCnt);
      System.out.println("   - Messages in response: " + inRespCnt);
      System.out.println("   - Messages not in response: " + (numMsgs - inRespCnt));
      System.out.println("   - Number of messages for every number of words: " );
      printMsgWordDist();
      
      /* Stats by message type */
      System.out.println("Stats for Subsets of Messages");
      System.out.println("Average message length by status:");
      if (publicCnt > 0) {
         System.out.println("Public:");
         System.out.println("   - By word count: " + avgWordCntPub);
         System.out.println("   - By character count: " + avgCharCntPub);
      }
      if (protectedCnt > 0) {
         System.out.println("Protected:");
         System.out.println("   - By word count: " + avgWordCntProt);
         System.out.println("   - By character count: " + avgCharCntProt);
      }
      if (privateCnt > 0) {
         System.out.println("- Private:");
         System.out.println("   - By word count: " + avgWordCntPriv);
         System.out.println("   - By character count: " + avgCharCntPriv);
      }
      
      System.out.println();
      System.out.println("Standard deviations of each average by status:");
      if (publicCnt > 0) {
         System.out.println("Public:");
         System.out.println("   - By word count: " + stdDevWordPub);
         System.out.println("   - By character count: " + stdDevCharPub);
      }
      if (protectedCnt > 0) {
         System.out.println("Protected:");
         System.out.println("   - By word count: " + stdDevWordProt);
         System.out.println("   - By character count: " + stdDevCharProt);
      }    
      if (privateCnt > 0) {
         System.out.println("Private:");
         System.out.println("   - By word count: " + stdDevWordPriv);
         System.out.println("   - By character count: " + stdDevCharPriv);
      }
      
      /* Recipient stats */
      System.out.println();
      System.out.println("Averages of messages for each recipient type:");
      if (avgSubWords > 0) {
         System.out.println("For subscriber recipients: ");
         System.out.println("   - Average word count: " + avgSubWords);
         System.out.println("   - Average char count: " + avgSubChars);
      }
      if (avgSelfWords > 0) {
         System.out.println("For self recipients: ");
         System.out.println("   - Average word count: " + avgSelfWords);
         System.out.println("   - Average char count: " + avgSelfChars);
      }
      if (avgUserWords > 0) {
         System.out.println("For user recipients: ");
         System.out.println("   - Average word count: " + avgUserWords);
         System.out.println("   - Average char count: " + avgUserChars);
      }
      if (avgAllWords > 0) {
         System.out.println("For all recipients: "); 
         /* Wording here is odd, all my variable names suck, do some find and replace*/
         System.out.println("   - Average word count: " + avgAllWords);
         System.out.println("   - Average char count: " + avgAllChars);
      }
      
      System.out.println("\nStandard deviations of messages for each recipient type: ");
      if (avgSubWords > 0) {
         System.out.println("For subscriber recipients:");
         System.out.println("   - Word count: " + subWordStdDev);
         System.out.println("   - Char count: " + subCharStdDev);
      }
      if (avgAllWords > 0) {
         System.out.println("For all recipients:");
         System.out.println("   - Word count: " + allWordStdDev);
         System.out.println("   - Char count: " + allCharStdDev);
      }
      if (avgUserWords > 0) {
         System.out.println("For user recipients:");
         System.out.println("   - Word count: " + userWordStdDev);
         System.out.println("   - Char count: " + userCharStdDev);
      }
      if (avgSelfWords > 0) {
         System.out.println("For self recipients:");
         System.out.println("   - Word count: " + selfWordStdDev);
         System.out.println("   - Char count: " + selfCharStdDev);
      }

      /* Conditional Histograms */
      System.out.println("\nHistogram of recipient values, by status");
      
      /* Public messages */
      System.out.println("Public messages:");
      System.out.println("   - All: " + pubAll);
      System.out.println("   - Self: " + pubSelf); 
      System.out.println("   - Subscribers: " + pubSub);       
      System.out.println("   - Users: " + pubUser);
      
      /* Protected messages */
      System.out.println("Protected messages:");
      System.out.println("   - All: " + protAll);
      System.out.println("   - Self: " + protSelf); 
      System.out.println("   - Subscribers: " + protSub);       
      System.out.println("   - Users: " + protUser);
          
      /* Private messages */        
      System.out.println("Protected messages:");
      System.out.println("   - All: " + privAll);
      System.out.println("   - Self: " + privSelf); 
      System.out.println("   - Subscribers: " + privSub);       
      System.out.println("   - Users: " + privUser);     
      
      /* Calculate total messages per recipient */
      int totalSubMsgs = pubSub + protSub + privSub;
      int totalSelfMsgs = pubSelf + protSelf + privSelf;
      int totalAllMsgs = pubAll + protAll + privAll;
      int totalUserMsgs = pubUser + protUser + privUser;
      
      System.out.println("In-response flag present for _/_ messages, by status value:");
      System.out.println("   - Public: " + pubResp + "/" + publicCnt);
      System.out.println("   - Protected: " + protResp + "/" + protectedCnt);
      System.out.println("   - Private: " + privResp + "/" + privateCnt);
            
      System.out.println("In-response flag present for _/_ messages, by recipient value:");
      System.out.println("   - All: " + allRespCnt + "/" + totalAllMsgs);
      System.out.println("   - Self: " + selfRespCnt + "/" + totalSelfMsgs);
      System.out.println("   - Subscribers: " + subRespCnt + "/" + totalSubMsgs);
      System.out.println("   - User: " + userRespCnt + "/" + totalUserMsgs);
       
   }
   
   private double calcStdDev(ArrayList<Integer> data, double mean) {
      double temp = 0;
      
      for (double d : data) {
          temp += (d-mean) * (d-mean);         
      }
      
      return Math.sqrt(temp / data.size());
   }
   
   private double calcAvg(ArrayList<Integer> nums) {
      int sum = 0;
      
      for (int i = 0; i < nums.size(); i++) {
         sum += nums.get(i);
      }
      
      return (double)sum / nums.size();
   }
   
   /* Need to print all possible message word lengths, not necessary though,
    * could add extra check inside printMsgWordDist instead, but this is easier
    */
   private void initMsgWordMap() {
      for (int i = 2; i <= 40; i++) {
         msgWordMap.put(i, 0);
      }   
   }
   
   private void printMsgWordDist() {   
      for (int i = 2; i <= 40; i++) {
         System.out.print(i + ": " + msgWordMap.get(i));
         System.out.println();
      }
   }
   
   private void processStatus(String status, String recip, int charLength, int wordLength, boolean inResp) {
      if (status.equals("private")) {
         numPrivChars += charLength;
         numPrivWords += wordLength;
         
         privChars.add(charLength);
         privWords.add(wordLength);
         
         privateCnt++;
         
         /* Private messages can be sent to "self" and a user ID */
         if (recip.equals("self")) {
            privSelf++;
         }
         else { 
            privUser++;
         }
         
         if (inResp) {
            privResp++;
         }
      }
      else if (status.equals("public")) {
         numPubChars += charLength;
         numPubWords += wordLength;
         
         pubChars.add(charLength);
         pubWords.add(wordLength);
         
         publicCnt++;
         
         /* Public messages can have all recipients */
         if (recip.equals("all")) {
            pubAll++;
         }
         else if (recip.equals("self")) {
            pubSelf++;
         }
         else if (recip.equals("subscribers")) {
            pubSub++;
         }
         else {
            pubUser++;
         }
         
         if (inResp) {
            pubResp++;
         }        
      }
      else if (status.equals("protected")) {
         numProtChars += charLength;
         numProtWords += wordLength;
         
         protChars.add(charLength);
         protWords.add(wordLength);
         
         protectedCnt++;
         
         /* Protected messages can be sent to subscribers, self, or a userID */
         if (recip.equals("subscribers")) {
            protSub++;
         }   
         else if (recip.equals("self")) {
            protSelf++;
         }
         else {
            protUser++;
         } 
         
         if (inResp) {
            protResp++;
         }                 
      }
      else {
         /* Error message */
         System.out.println("Error processing message with status: " + status);
         System.exit(1);
      }
   }
      
   private void processUser(String userId) {
      users.add(userId);
   }
   
   private void processRecipient(String recipient, int numWords, int numChars, boolean inResponse) {
      if (recipients.containsKey(recipient)) {
         recipients.put(recipient, recipients.get(recipient) + 1);
      }
      else {
         recipients.put(recipient, 1);
      }

      if (inResponse) {
         responseWords.add(numWords);
         responseChars.add(numChars);
      }
      else {
         noResponseWords.add(numWords);
         noResponseChars.add(numChars);
      }
      /* How to now compute the averages and std dev?
       * Need to add counts to respective list
       */
       /* possible recipient types: {subscribers, self, all, a userId} */

      if (recipient.equals("subscribers")) {
         subWordCount.add(numWords);
         subCharCount.add(numChars);
         if (inResponse == true) {
            subRespCnt++;
         }
      }
      else if (recipient.equals("self")) {
         selfWordCount.add(numWords);
         selfCharCount.add(numChars);
         if (inResponse == true) {
            selfRespCnt++;
         }
      }
      else if (recipient.equals("all")) {
         allWordCount.add(numWords);
         allCharCount.add(numChars);
         if (inResponse == true) {
            allRespCnt++;
         }
      }
      else {/* UserID recipient presumably */
         userWordCount.add(numWords);
         userCharCount.add(numChars);
         if (inResponse == true) {
            userRespCnt++;
         }
      }
   
   }

}
