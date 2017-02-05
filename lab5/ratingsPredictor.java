import com.mongodb.MongoClient;

public class ratingsPredictor {

   private static String server = "cslvm31.csc.calpoly.edu";
   private static int port = 27017;

   public static void main(String args[]) {

         try {
            MongoClient client = new MongoClient(server, port); // establishing the connection 
                                                                // using two argument constructor
         } catch (Exception e) {
            System.out.println(e.toString());
         }

   }

}
