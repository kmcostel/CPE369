import com.mongodb.*;
import com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
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


import java.util.Arrays;
import com.mongodb.Block;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.List;




public class ratingsPredictor {

    private static final String server = "cslvm31.csc.calpoly.edu";
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
            // Connect to the MongoDB server
            client = new MongoClient(server, port); 
           
            // Authenticate
            JSONParser parser = new JSONParser();
           
            // Parse the 'user.auth' file
            Object obj = parser.parse(new FileReader(authFile));
            JSONObject jsonObj = (JSONObject) obj;
            String authDb = (String) jsonObj.get("authDb");
            String user  = (String) jsonObj.get("user");
            String password = (String) jsonObj.get("password");
            String db = (String) jsonObj.get("db");
           
            // check this out --> http://api.mongodb.com/java/current/com/mongodb/MongoClientURI.html
            MongoDatabase dbAuth = client.getDatabase(authDb);
            boolean auth = dbAuth.authenticate(user, password);
           
            // Print error message if the authentication was not successful
            if (!auth) {
                System.out.println("ERROR: Could not authenticate user");
                System.exit(1);
            }
           
            // Check collection existence
            MongoIterable<String> collNames = db.listCollectionNames();
            for (String name : collNames) {
                if (name.equals(ratingsCollName)) {
                    ratingsCollFound = true;
                }
            }
           
            // Upload the ratings dataset if the collection does not exist
            if (!ratingsCollFound) {
                db.createCollection(ratingsCollName);
                ratingsColl = db.getCollection(ratingsCollName);
                ratingsColl.insertMany(Files.readAllLines(jsonFileName, Charset.forName("US-ASCII")));    
            }
           
        } 
        catch (Exception e) {
            System.out.println("EXCEPTION CAUGHT: " + e.toString());
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
    }
   
    /* http://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/aggregation/ */
    
    /* Query the database to find this user's document, then access their rating array */
    private static double getUserRatings (String userId) {
       
       ratingsColl.aggregate(
           Arrays.asList(
               new Document("$match", new Document("username", userId))
           )
       );
       
    }
   
   /* Average movie rating & in the similarity calculation. 
      Input will be one of the private movieN lists.
      Very generic, can be used for any list average */
   private static double getMovieAvg (int movieNdx) {
      
   }
      

}
