import com.mongodb.*;
import com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser.*;

import java.util.ArrayList;
import org.json.simple.*;
import java.io.*;
import java.lang.Math;
import java.lang.Object;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.Arrays;
import com.mongodb.Block;
import com.mongodb.MongoCredential;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.List;




public class ratingsPredictor {

    private static final String server = "localhost";
    private static final int port = 27017;
    private static final String authFile = "user.auth";
    private static final int NUM_MOVIES = 13;
    private static final String ratingsCollName = "ratings";
    private static final String jsonFileName = "ratings350.json";
    private static MongoClient client;
    private static MongoCollection<Document> ratingsColl;
   
    private static boolean ratingsCollFound;

   
    public static void main(String args[]) throws FileNotFoundException, IOException {
        try {
            // Authenticate
            JSONParser parser = new JSONParser();
           
            // Parse the 'user.auth' file
            Object obj = parser.parse(new FileReader(authFile));
            JSONObject jsonObj = (JSONObject) obj;
            String authDb = (String) jsonObj.get("authDb");
            String user  = (String) jsonObj.get("user");
            char[] password = ((String) jsonObj.get("password")).toCharArray();
            String db = (String) jsonObj.get("db");
            
            System.out.println("Done parsing auth file");

            // Connect to the MongoDB server
            ServerAddress seed = new ServerAddress(server, port);
            MongoCredential cred = MongoCredential.createCredential(user, authDb, password);
            // client = new MongoClient(server);
            client = new MongoClient(seed, Arrays.asList(cred));          
           
            System.out.println("Done connecting to server");

            // Switch to the specified database
            MongoDatabase userDb = client.getDatabase(db);
            System.out.println("Done getting database " + db);

            // Check collection existence; upload the ratings dataset if the collection does not exist
            MongoIterable<String> collNames = userDb.listCollectionNames();
            collNames.forEach(new Block<String>() {
              @Override
              public void apply(final String nm) {
                if (nm.equals(ratingsCollName)) {
                  System.out.println("match found");
                  ratingsCollFound = true;
                }
              }
              
            }); 


            // if (!(userDb.collectionExists(ratingsCollName))) {
            if (!ratingsCollFound) {
              System.out.println("Could not find collection " + ratingsCollName);
                userDb.createCollection(ratingsCollName, null);
                ratingsColl = userDb.getCollection(ratingsCollName);

                // List<Document> docs = new List<Document>();
                Scanner sc = new Scanner(new File(jsonFileName));

                while (sc.hasNext()) {
                    System.out.println(sc.next());
                }
                // Document doc = new Document();
                // List<String> entries = Files.readAllLines(Paths.get(jsonFileName), Charset.forName("US-ASCII"));


                // ratingsColl.insertOne(doc.parse((Files.readAllLines(Paths.get(jsonFileName), Charset.forName("US-ASCII"))).get(0))); 
                // ratingsColl.insertMany((DBObject)JSON.parse(new String(Files.readAllBytes(Paths.get(jsonFileName)))));  

                // DBCursor cursor = ratingsColl.find();
                // while (cursor.hasNext()) {
                //   System.out.println(cursor.next());
                // } 
             }
           
        } 
        catch(Exception e) {
          System.out.println("ERROR: " + e.toString());
        }
       
    }
   
   
   
   /* Follows the math formula given */
   private static double sim(ArrayList<Integer> xRats, ArrayList<Integer> yRats) {
       // double xAvg = getAvg(xRats);
       // double yAvg = getAvg(yRats);
       
       // double numerator = 0.0;
       
       // double xDenomSum = 0.0;
       // double yDenomSum = 0.0;
       
       // double denominator = 0.0;
       
       // for (int i = 0; i < NUM_MOVIES; i++) {
       //    numerator += (xRats.get(i) - xAvg) * (yRats.get(i) - yAvg);
          
       //    xDenomSum += ((xRats.get(i) - xAvg) * (xRats.get(i) - xAvg));
       //    yDenomSum += ((yRats.get(i) - yAvg) * (yRats.get(i) - yAvg));
       // }
       
       // denominator = Math.sqrt(xDenomSum) * Math.sqrt(yDenomSum);
       
       // return numerator / denominator;

      return Double.MAX_VALUE;        
   }
   
   /* Average user rating */
   private static double avgUserRating(ArrayList<Double> ratings) {
       int nonZeroRatings = 0;
       double sumRatings = 0.0;
       
       for (int i = 0; i < ratings.size(); i++) {
           if (ratings.get(i) > 0.0) {
               sumRatings += ratings.get(i);
               nonZeroRatings++;
           }
       }
       
       /* Compute average of movie's the user has seen (average of non-zero ratings) */   
       return sumRatings / nonZeroRatings;   
   }
   
   /* Query to retrieve this particular user's document...? */
    private static double getNormFactor (String userId, int movieNdx) {
        //ArrayList<Double> getUserRatings(userId);
      return Double.MAX_VALUE;
    }
   
    /* http://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/aggregation/ */
    
    /* Query the database to find this user's document, then access their rating array */
    private static double getUserRatings (String userId) {
       
      DBObject match = new BasicDBObject();
      DBObject usrname = new BasicDBObject();
      //obj.put("$match", new 

       // ratingsColl.aggregate(
       //     Arrays.asList(
       //         new Document("$match", new Document("username", userId))
       //     )
       // );
      return Double.MAX_VALUE; 
    }
   
   /* Average movie rating & in the similarity calculation. 
      Input will be one of the private movieN lists.
      Very generic, can be used for any list average */
   private static double getMovieAvg (int movieNdx) {
      
      return Double.MAX_VALUE;
   }
      

}
