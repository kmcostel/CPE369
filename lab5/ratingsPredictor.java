import com.mongodb.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.*;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser.*;

import java.util.ArrayList;
import java.util.Scanner;
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
    private static final String jsonFileName = "ratings350.json";
    private static MongoClient client;
    private static MongoCollection<Document> ratingsColl;
   
    private static String ratingsCollName;
    private static boolean ratingsCollFound;

   
    public static void main(String args[]) throws FileNotFoundException, IOException {
      try {
          /* Authenticate */
          JSONParser parser = new JSONParser();
         
          /* Parse the 'user.auth' file */
          Object obj = parser.parse(new FileReader(authFile));
          JSONObject jsonObj = (JSONObject) obj;
          String authDb = (String) jsonObj.get("authDb");
          String user  = (String) jsonObj.get("user");
          char[] password = ((String) jsonObj.get("password")).toCharArray();
          ratingsCollName = (String) jsonObj.get("db");
          
          /* Connect to the MongoDB server */
          ServerAddress seed = new ServerAddress(server, port);
          MongoCredential cred = MongoCredential.createCredential(user, authDb, password);
          client = new MongoClient(seed, Arrays.asList(cred));          
         
          /* Switch to the user's database */
          MongoDatabase userDb = client.getDatabase(user);

          /* Check collection existence */
          MongoIterable<String> collNames = userDb.listCollectionNames();
          collNames.forEach(new Block<String>() {
            @Override
            public void apply(final String nm) {
              if (nm.equals(ratingsCollName)) {
                ratingsCollFound = true;
                ratingsColl = userDb.getCollection(ratingsCollName);
              }
            }             
          }); 

          /* Create and populate the ratings collection since it does not already exist */
          if (!ratingsCollFound) {
            userDb.createCollection(ratingsCollName, new CreateCollectionOptions());
            ratingsColl = userDb.getCollection(ratingsCollName);

            /* Iterate over the .json file and insert the objects one by one */
            Document doc = new Document();
            Scanner sc = new Scanner(new File(jsonFileName));
            while (sc.hasNext()) {
              ratingsColl.insertOne(doc.parse(sc.next()));
            }
          } 
          
          int rid = 20;
          getAvgUserRating(rid, 5);
          getAvgMovieRating(rid, 5);
          //getUserRatingsArr(rid); 
          getSim(rid, 5);    
      } 
      catch(Exception e) {
        System.out.println("EXCEPTION CAUGHT: " + e.toString());
      }
    }
   
   
    
    /* Queries the database to find the document of the user with |rid|,
     * then averages the user's movie ratings; excludes the rating of the movie with |movieId|.
     * |movieId| is a value between 0 - 12 
     */
    private static double getAvgUserRating (int rid, int movieId) { 
      double avgRating = 0.0 ;
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(ne("idx", movieId)),
            match(gt("ratings", 0.0)),
            group("$_id", avg("avgRating", "$ratings"))
          )
      ); 
      
      for (Document dbObj : output) {
        avgRating = (double)dbObj.get("avgRating");
      }
      
      System.out.println("avgUserRating = " + avgRating);
      
      return avgRating;  
    }
    
    /* Calculates the average movie rating that everyone else 
     * besides this user has given the movieNdx 
     */
    private static double getAvgMovieRating (int rid, int movieId) {
      double avgRating = 0.0;
      
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(ne("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", movieId)),
            match(gt("ratings", 0.0)),
            group(null, avg("avgRating", "$ratings"))
          )
      );
        
      for (Document dbObj : output) {
        avgRating = (double)dbObj.get("avgRating");
      }
      
      System.out.println("avgMovieRating = " + avgRating);
          
      return avgRating; 
    }
    
    /* Weighted sum predictor */
    private static double getSim(int rid, int movieId) {
      
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            // Project |rid| and |movieId| onto every document in the collection...
            project(fields(
              excludeId(), 
              include("ratings", "RID"),
              computed("refRid", new Document("$literal", rid)), 
              computed("movieId", new Document("$literal", movieId))
             )
            ),
            lookup(
              "ratings", 
              "refRid",
              "RID",
              "refObj"  
            ),
            match(ne("RID", rid)),
            project(fields(
              include("RID"),
              computed("xRats", "$refObj.ratings"),
              computed("yRats", "$ratings")           
             )
            ),
            unwind("$xRats"),
            unwind("$xRats"),
            group(
              "$RID", 
              first("yRats", "$yRats"), 
              Accumulators.push("xRats", "$xRats"), 
              avg("avgXRats", "$xRats")
            ),
            unwind("$yRats"),
            group("$_id", first("avgXRats", "$avgXRats"), first("xRats", "$xRats"), Accumulators.push("yRats", "$yRats"), avg("avgYRats", "$yRats")),
            unwind("$xRats", new UnwindOptions().includeArrayIndex("xIdx")),
            unwind("$yRats", new UnwindOptions().includeArrayIndex("yIdx")),
            project(fields(
              include("_id", "xRats", "avgXRats", "yRats", "avgYRats", "xIdx", "yIdx"),
              computed("xDiff", new Document("$subtract", Arrays.asList("$xRats", "$avgXRats"))),
              computed("yDiff", new Document("$subtract", Arrays.asList("$yRats", "$avgYRats"))),
              computed("idxDiff", new Document("$subtract", Arrays.asList("$xIdx", "$yIdx")))
             )
            ),
            match(eq("idxDiff", 0)),
            project(fields(
              include("xRats", "avgXRats", "yRats", "avgYRats", "xIdx", "yIdx", "xDiff", "yDiff"),
              computed("prod", new Document("$multiply", Arrays.asList("$xDiff", "$yDiff"))),
              computed("xDiffSqrd", new Document("$multiply", Arrays.asList("$xDiff", "$xDiff"))),
              computed("yDiffSqrd", new Document("$multiply", Arrays.asList("$yDiff", "$yDiff")))
             )
            ),
            group(
              "$_id", 
              sum("numer", "$prod"),
              sum("xDiffsSqrd", "$xDiffSqrd"),
              sum("yDiffsSqrd", "$yDiffSqrd")   
            ),   
            project(fields(
              include("numer"),
              computed("xsSqrt", new Document("$sqrt", "$xDiffsSqrd")),
              computed("ysSqrt", new Document("$sqrt", "$yDiffsSqrd"))             
             )
            ),
            project(fields(
              include("numer"),
              computed("denom", new Document("$multiply", Arrays.asList("$xsSqrt", "$ysSqrt")))
             )
            ),
            project(
              computed("sim", new Document("$divide", Arrays.asList("$numer", "$denom")))
            )                   
          )
      );
        
       double avgRating = 0.0;
       int count = 0;
       for (Document dbObj : output) {
         System.out.println(dbObj);
         count++;
       }
      
      System.out.println("Count: " + count);
      return avgRating;
    }
    
    
    
    /*private static double[] getUserRatingsArr(int rid) {
      double[] ratingsArr = new double[NUM_MOVIES];
      
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid))
          )
      ); 
      
      Document dbObj = output.first();
      
      System.out.println((double[])dbObj.get("ratings"));
      return (double[])dbObj.get("ratings");  
    }*/
    
       
   /* Follows the math formula given */
   /*private static double sim(ArrayList<Integer> xRats, ArrayList<Integer> yRats) {
     double xAvg = getAvg(xRats);
     double yAvg = getAvg(yRats);
     
     double numerator = 0.0;
     
     double xDenomSum = 0.0;
     double yDenomSum = 0.0;
     
     double denominator = 0.0;
     
     for (int i = 0; i < NUM_MOVIES; i++) {
        numerator += (xRats.get(i) - xAvg) * (yRats.get(i) - yAvg);
        
        xDenomSum += ((xRats.get(i) - xAvg) * (xRats.get(i) - xAvg));
        yDenomSum += ((yRats.get(i) - yAvg) * (yRats.get(i) - yAvg));
     }
     
     denominator = Math.sqrt(xDenomSum) * Math.sqrt(yDenomSum);
     
     return numerator / denominator;  
   }*/
      
   /* Query to retrieve this particular user's document...?
    private static double getNormFactor (int rid, int movieNdx) {
    
      return Double.MAX_VALUE;
    }*/
}