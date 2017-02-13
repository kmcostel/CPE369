/*
 * Authors: Kevin Costello, Holly Haraguchi
 * CPE 369, Lab 5, Winter 2017
 */

import com.mongodb.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.*;
import static com.mongodb.client.model.Sorts.descending;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser.*;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.Iterator;
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

import org.bson.codecs.configuration.CodecConfigurationException;
import static com.mongodb.client.model.BsonField.*;



public class ratingsPredictor {

    private static final String server = "localhost";
    private static final int port = 27017;
    private static final String authFile = "user.auth";
    private static final int NUM_MOVIES = 13;
    private static final int N = 10;
    private static final String jsonFileName = "ratings350.json";
    private static MongoClient client;
    private static MongoCollection<Document> ratingsColl;
   
    private static String ratingsCollName;
    private static boolean ratingsCollFound;
    private static MongoDatabase userDb;

    private static String simCollNm = "similarities";
    
    public static void main(String args[]) throws FileNotFoundException, IOException {
      if (args.length == 1) {
        String jsonInFile = args[0];
                           
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
            userDb = client.getDatabase(user);

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
              userDb.createCollection(ratingsCollName);
              ratingsColl = userDb.getCollection(ratingsCollName);

              /* Iterate over the .json file and insert the objects one by one */
              Document doc = new Document();
              Scanner sc = new Scanner(new File(jsonFileName));
              while (sc.hasNext()) {
                ratingsColl.insertOne(doc.parse(sc.next()));
              }
            } 
            
            // Parse the JSON input file
            int rid = -1;
            int mid = -1;
            
            List<String> inputLines = Files.readAllLines(Paths.get(jsonInFile), Charset.forName("US-ASCII"));
            String single = String.join("", inputLines);
            
            JSONArray jsonIn = (JSONArray)parser.parse(single);
            Iterator<JSONObject> iter = jsonIn.iterator();
            while (iter.hasNext()) {
              JSONObject object = iter.next();
              rid = (int)((long)object.get("RID"));
              mid = (int)((long)object.get("Movie"));

              /* Generate similarities between user RID and everyone else */
              genSim(rid, mid);
              /* Add C (Normalizing factor) attribute to every document in similarities collection */
              insertCVal();
              /* Calculate predictions */
              System.out.println(genPredictions(rid, mid)); 
            }           
            
                       
        } 
        catch(Exception e) {
          System.out.println("EXCEPTION CAUGHT: " + e.toString());
        }
      }
      else {
        System.out.println("Usage: ./runPredictor.sh <jsonInputFileName>");
      }
    }  

    private static String getFirstName(int rid) {
    
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid)),
            project(fields(
              computed("first", "$respondent.name.first")
            ))
          )
      ); 
      
      String firstName = "";
      for (Document dbObj : output) {
        firstName = (String)dbObj.get("first");
      }
            
      return firstName; 
    
    }
    
    private static String getLastName(int rid) {
    
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid)),
            project(fields(
              computed("last", "$respondent.name.last")
            ))
          )
      ); 
      
      String lastName = "";
      for (Document dbObj : output) {
        lastName = (String)dbObj.get("last");
      }
            
      return lastName;
    }
    
    private static String getMovieTitle(int movieId) {
       if (movieId == 0) {
         return "Star Wars: A New Hope";
       }
       else if (movieId == 1) {
         return "Godfather";
       }
       else if (movieId == 2) {
         return "Memento";
       }
       else if (movieId == 3) {
         return "Saw";
       }
       else if (movieId == 4) {
         return "Rocky";
       }
       else if (movieId == 5) {
         return "Princess Bride";
       }
       else if (movieId == 6) {
         return "Sleepless in Seattle";
       }
       else if (movieId == 7) {
         return "Pretty Woman";
       }
       else if (movieId == 8) {
         return "Avatar";
       }
       else if (movieId == 9) {
         return "Dogma";
       }
       else if (movieId == 10) {
         return "Batman Begins";
       }
       else if (movieId == 11) {
          return "Suicide Squad";
       }
       
       /* movieId == 12 */
       return "Beverly Hills Cop";
    }
    
    /* Get the user rating for movie with id |movieId| */
    private static double getRating(int rid, int mid) {
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", mid))
          )
      );
      
      double rating = 0.0;
      for (Document dbObj : output) {
        rating = (double)dbObj.get("ratings");
      }
            
      return rating;
    }

    
    private static String genPredictions(int rid, int mid) {
      
      double real = getRating(rid, mid);
      boolean hasSeen = real > 0.0 ? true : false;
      
      double avgUserRating = getAvgUserRating(rid, mid);
      double avgMovieRating = getAvgMovieRating(rid, mid);
      
      double weightedSum = getWeightedSum(rid, mid);
      double adjWeightedSum = getAdjWeightSum(rid, mid);  
      
      double avgMovieRatingN = getAvgMovieRatingN(rid, mid, N);
      double weightSumN = getWeightedSumN(rid, mid, N);
      double adjWeightedSumN = getAdjWeightSumN(rid, mid, N);
    
    
      String first = getFirstName(rid);
      String last = getLastName(rid);
      String title = getMovieTitle(mid);
      
      String ratPre = "";
      
      if (hasSeen == false) {
        ratPre = "{\n";
        ratPre += "RID: " + rid + ",\n";
        ratPre += "respondent: {first: "+first+", last: "+last+"},\n";
        ratPre += "MovieId: "+mid+",\n";
        ratPre += "MovieTitle: "+title+",\n";
        ratPre += "predictions: [ {type: \"Average User\", prediction: "+avgUserRating + "},\n";
        ratPre += "{type: \"Average Movie\", prediction: " + avgMovieRating + "},\n";
        ratPre += "{type: \"Weighted sum\", prediction: " + weightedSum + "},\n";
        ratPre += "{type: \"Adjusted weighted Sum\", prediction: " + adjWeightedSum + "},\n";
        ratPre += "{type: \"NNN Average User\", N: " + N + ", prediction: " + avgMovieRatingN + "},\n";
        ratPre += "{type: \"NNN Weighted Sum\", N: " + N + ", prediction: " + weightSumN + "},\n";
        ratPre += "{type: \"NNN adjusted weighted sum\", N: " + N + ", prediction: " + adjWeightedSumN + "},\n";
        ratPre += "]\n}\n";
      }
      else {
        ratPre = "{\n";
        ratPre += "RID: " + rid + ",\n";
        ratPre += "respondent: {first: "+first+", last: "+last+"},\n";
        ratPre += "MovieId: "+mid+",\n";
        ratPre += "MovieTitle: "+title+",\n";
        ratPre += "predictions: [ {type: \"Average User\", prediction: "+avgUserRating + ", error: " + (real - avgUserRating) + "},\n";
        ratPre += "{type: \"Average Movie\", prediction: " + avgMovieRating + ", error: " + (real - avgMovieRating) + "},\n";
        ratPre += "{type: \"Weighted sum\", prediction: " + weightedSum + ", error: " + (real - weightedSum) + "},\n";
        ratPre += "{type: \"Adjusted weighted Sum\", prediction: " + adjWeightedSum + ", error: " + (real - adjWeightedSum) + "},\n";
        ratPre += "{type: \"NNN Average User\", N: " + N + ", prediction: " + avgMovieRatingN + ", error: " + (real - avgMovieRatingN) + "},\n";
        ratPre += "{type: \"NNN Weighted Sum\", N: " + N + ", prediction: " + weightSumN + ", error: " + (real - weightSumN) + "},\n";
        ratPre += "{type: \"NNN adjusted weighted sum\", N: " + N + ", prediction: " + adjWeightedSumN + ", error: " + (real - adjWeightedSumN) + "},\n";
        ratPre += "]\n}\n";
      }


      return ratPre;
    }

    
    /* Queries the database to find the document of the user with |rid|,
     * then averages the user's movie ratings; excludes the rating of the movie with |movieId|.
     * |movieId| is a value between 0 - 12 
     */
    private static double getAvgUserRating (int rid, int movieId) { 
      double avgUserRating = 0.0 ;
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(eq("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(ne("idx", movieId)),
            match(gt("ratings", 0.0)),
            group("$_id", avg("avgUserRating", "$ratings"))
          )
      ); 
      
      for (Document dbObj : output) {
        avgUserRating = (double)dbObj.get("avgUserRating");
      }
            
      return chopRating(avgUserRating);  
    }
    
    /* Calculates the average movie rating that everyone else 
     * besides this user has given the movieNdx 
     */
    private static double getAvgMovieRating (int rid, int movieId) {
      double avgMovieRating = 0.0;
      
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(ne("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", movieId)),
            match(gt("ratings", 0.0)),
            group(null, avg("avgMovieRating", "$ratings"))
          )
      );
        
      for (Document dbObj : output) {
        avgMovieRating = (double)dbObj.get("avgMovieRating");
      }
                
      return chopRating(avgMovieRating); 
    }
    
    /* Returns the predicted weighted sum rating */
    private static double getWeightedSum(int rid, int movieId) {
      
      double wsRating = 0.0;
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),   
            match(eq("idx", movieId)),
            match(gt("ratings", 0.0)), 
            match(ne("RID", rid)),           
            lookup("similarities", "RID", "_id", "refObj"),
            unwind("$refObj"),
            project(fields(
              excludeId(), 
              include("ratings", "RID"),
              computed("sim", "$refObj.sim"),
              computed("cVal", "$refObj.cVal")
            )),
            project(fields(
              include("cVal"),
              computed("simMultRating", new Document("$multiply", Arrays.asList("$sim", "$ratings")))
            )),
            group(null, sum("sum", "$simMultRating"), first("cVal", "$cVal")),   
            project(fields(
              computed("weightedSumRating", new Document("$multiply", Arrays.asList("$sum", "$cVal")))
            ))  
          )
      ); 
      
      for (Document dbObj : output) {
        wsRating = (double)dbObj.get("weightedSumRating");
      }
            
      return chopRating(wsRating);
    }
       
    
    // Inserts the calculated cVal into every document into the |similarites| collection
    private static void insertCVal() {
      double cVal = 0.0;
      
      MongoCollection<Document> simColl = userDb.getCollection(simCollNm);
             
      AggregateIterable<Document> output = simColl.aggregate(
        Arrays.asList(
          project(computed("sim", new Document("$abs", "$sim"))), 
          group(null, sum("cVal", "$sim"))
        )
      );
      
      // Delete |normFac|, the normalizing factor collection, if it exists already
      String normNm = "normFac";
      MongoIterable<String> collNames = userDb.listCollectionNames();
      collNames.forEach(new Block<String>() {
        @Override
           public void apply(final String nm) {
             if (nm.equals(normNm)) {
               userDb.getCollection(normNm).drop();
             }
           }             
      });
        
      // Create and populate |normFac| (normalizing factor) collection      
      userDb.createCollection(normNm);
      MongoCollection<Document> normColl = userDb.getCollection(normNm);                         
      
      /* Expect only one document */
      for (Document dbObj : output) {
        normColl.insertOne(dbObj);
        cVal = 1 / ((double)dbObj.get("cVal"));
      }
     
      /* Adds normalizing factor field to every document in the similarity collection */
      simColl.updateMany(new Document(), new Document("$set", new Document("cVal", cVal))); 
    }
    
    /* Generates similarity between users.
     * Inserts the values into a new Mongo collection called |similarities|.
     * If a collection with the same name already exists, it is deleted.     
     */
    private static void genSim(int rid, int movieId) {      
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
            group("$_id", first("avgXRats", "$avgXRats"), first("xRats", "$xRats"), Accumulators.push("yRats", "$yRats"),    avg("avgYRats", "$yRats")),
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
            match(gt("xRats", 0)),
            match(gt("yRats", 0)),
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
               
      // Delete similarities collection if it already exists
      MongoIterable<String> collNames = userDb.listCollectionNames();
      collNames.forEach(new Block<String>() {
        @Override
           public void apply(final String nm) {
             if (nm.equals(simCollNm)) {
               userDb.getCollection(simCollNm).drop();
             }
           }             
      });
        
      // Create and populate the |similarities| collection         
      userDb.createCollection(simCollNm);
      MongoCollection<Document> simColl = userDb.getCollection(simCollNm);           
                  
      for (Document dbObj : output) {
        simColl.insertOne(dbObj);
      }
    } 
    
    // Returns the predicted adjusted weight sum rating
    private static double getAdjWeightSum(int rid, int movieId) {
      double adjWsRating = 0.0;
      double userAvg = getAvgUserRating(rid, movieId);
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(ne("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(gt("ratings", 0.0)),           
            group("$RID", avg("avgRating", "$ratings"), first("RID", "$RID")),
            lookup("ratings", "RID", "RID", "self"),
            unwind("$self"),
            project(fields(
              include("RID", "avgRating"),
              computed("ratings", "$self.ratings")            
            )),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", movieId)),
            project(fields(
              include("RID", "avgRating"),
              computed("rDiff", new Document("$subtract", Arrays.asList("$ratings", "$avgRating")))
            )),            
            lookup("similarities", "RID", "_id", "simObj"),
            unwind("$simObj"),
            project(fields(
              include("RID", "rDiff"),
              computed("sim", "$simObj.sim"),
              computed("cVal", "$simObj.cVal")
            )),
            project(fields(
              include("RID", "rDiff", "sim", "cVal"),
              computed("simMultDiff", new Document("$multiply", Arrays.asList("$sim", "$rDiff")))
            )),
            group(null, sum("sum", "$simMultDiff"), first("cVal", "$cVal")),
            project(fields(
              computed("cMultSum", new Document("$multiply", Arrays.asList("$cVal", "$sum"))),
              computed("userAvg", new Document("$literal", userAvg))
            )),
            project(     
              computed("adjWsRating", new Document("$sum", Arrays.asList("$userAvg", "$cMultSum")))
            )
          )  
      ); 

      for (Document dbObj : output) {
        adjWsRating = (double)dbObj.get("adjWsRating");
      }

      return chopRating(adjWsRating);
    }  
    
    /*
     * N Nearest Neighbors Predictors
     */

    // Average movie rating
    private static double getAvgMovieRatingN (int rid, int movieId, int n) {
      double avgMovieRating = 0.0;
      
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(ne("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", movieId)),
            match(gt("ratings", 0.0)),
            lookup("similarities", "RID", "_id", "simObj"),
            unwind("$simObj"),
            project(fields(
                include("RID", "ratings"),
                computed("sim", "$simObj.sim") 
            )),
            sort(descending("sim")),
            limit(n),
            group(null, avg("avgMovieRating", "$ratings"))
          )
      );
        
      for (Document dbObj : output) {
        //System.out.println(dbObj);
        avgMovieRating = (double)dbObj.get("avgMovieRating");
      }
                
      return chopRating(avgMovieRating); 
    }
    
    // Weighted sum predictor
    private static double getWeightedSumN(int rid, int movieId, int n) {
      double wsRating = 0.0;
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),   
            match(eq("idx", movieId)),
            match(gt("ratings", 0.0)), 
            match(ne("RID", rid)),           
            lookup("similarities", "RID", "_id", "refObj"),
            unwind("$refObj"),
            project(fields(
              excludeId(), 
              include("ratings", "RID"),
              computed("sim", "$refObj.sim"),
              computed("cVal", "$refObj.cVal")
            )),
            sort(descending("sim")),
            limit(n),
            project(fields(
              include("cVal"),
              computed("simMultRating", new Document("$multiply", Arrays.asList("$sim", "$ratings")))
              )
            ),
            group(null, sum("sum", "$simMultRating"), first("cVal", "$cVal")),   
            project(fields(
              computed("weightedSumRating", new Document("$multiply", Arrays.asList("$sum", "$cVal")))
            ))
          )
      ); 
      for (Document dbObj : output) {
        wsRating = (double)dbObj.get("weightedSumRating");
      }
            
      return chopRating(wsRating);
    }

    // Adjusted weighted sum predictor
    private static double getAdjWeightSumN(int rid, int movieId, int n) {
      double adjWsRating = 0.0;
      double userAvg = getAvgUserRating(rid, movieId);
         
      AggregateIterable<Document> output = ratingsColl.aggregate(
          Arrays.asList(
            match(ne("RID", rid)),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(gt("ratings", 0.0)),           
            group("$RID", avg("avgRating", "$ratings"), first("RID", "$RID")),
            lookup("ratings", "RID", "RID", "self"),
            unwind("$self"),
            project(fields(
              include("RID", "avgRating"),
              computed("ratings", "$self.ratings")            
             )),
            unwind("$ratings", new UnwindOptions().includeArrayIndex("idx")),
            match(eq("idx", movieId)),
            project(fields(
              include("RID", "avgRating"),
              computed("rDiff", new Document("$subtract", Arrays.asList("$ratings", "$avgRating")))
             )),            
            lookup("similarities", "RID", "_id", "simObj"),
            unwind("$simObj"),
            project(fields(
              include("RID", "rDiff"),
              computed("sim", "$simObj.sim"),
              computed("cVal", "$simObj.cVal")
             )),
            sort(descending("sim")),
            limit(n),
            project(fields(
              include("RID", "rDiff", "sim", "cVal"),
              computed("simMultDiff", new Document("$multiply", Arrays.asList("$sim", "$rDiff")))
              )),           
            group(null, sum("sum", "$simMultDiff"), first("cVal", "$cVal")),
            project(fields(
              computed("cMultSum", new Document("$multiply", Arrays.asList("$cVal", "$sum"))),
              computed("userAvg", new Document("$literal", userAvg))
             )),
            project(     
              computed("adjWsRating", new Document("$sum", Arrays.asList("$userAvg", "$cMultSum")))
            )
          )
      ); 
      
      for (Document dbObj : output) {   
        adjWsRating = (double)dbObj.get("adjWsRating");
      }
          
      return chopRating(adjWsRating);
    }  
   
    /* Round |rating| up or down to fit within the rating bounds (1.0 - 10.0) */ 
    private static double chopRating(double rating) {
      if (Double.compare(rating, 10.0) > 0) {
        return 10.0;
      }
      else if (Double.compare(rating, 1.0) < 0) {
        return 1.0;
      }

      return rating;
    }
}
