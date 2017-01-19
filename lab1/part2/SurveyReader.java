/* 
 * Lab 1-2
 * Authors: Holly Haraguchi (hharaguc@calpoly.edu) and Kevin Costello (kmcostel@calpoly.edu)
 * CPE 369, Winter 2016
 */
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import java.io.*;
import java.util.*;
import java.lang.Double;
 
public class SurveyReader {
    /* MR4 - Population Statistics */
    /* Survey respondents */
    private int numResps;
    private int numFemales;
    private int numMales;
    private int numNa;
    
    /* Age distribution */
    private int numUnder20;
    private int num20To29;
    private int num30To39;
    private int num40To49;
    private int num50To59;
    private int num60To69;
    private int numOver70;
    
    /* Income groups */
    private int numInc0;
    private int numInc1;
    private int numInc2;
    private int numInc3;
    private int numInc4;
    private int numInc5;
    
    /* Education levels */
    private int numEdu0;
    private int numEdu1;
    private int numEdu2;
    private int numEdu3;
    private int numEdu4;
    private int numEdu5;

    /* Geographic distribution - regions */
    private int numNe;
    private int numMw;
    private int numSouth;
    private int numWest;
    private int numPac;
    
    private List<String> neStates;
    private List<String> mwStates;
    private List<String> souStates;
    private List<String> westStates;
    private List<String> pacStates;
    
    /* MR5 - Movie Rating */
    private int numNonZeroRatings;
    /* 13 indexes for 13 movies, index of that movie keeps track of total rating count */
    double[] movieRatingSums;
    double[] stdDevRatings;
    
    /* MR6 - Movie ratings by gender */
    /* total number of nonzero movie ratings by gender value */
    int maleNonZeroCount;
    int femaleNonZeroCount;
    int naNonZeroCount;
    /* Used to calculate movie rating average for each gender value; these arrays will hold sums,
       divide by above count of non zero ratings to get average*/
    double[] maleRatings;
    double[] femaleRatings;
    double[] naRatings;
    
    /* File I/O */
    private String inFileName;
    private String outFileName;
    private static int NUM_MOVIES = 13;
    
    /* Constructor without specified output file -- just print to console */
    public SurveyReader(String JSONFile) {
        inFileName = JSONFile;
        outFileName = new String();
        initArrsAndLists();
        parseJSONFile();
    }
   
   /* Constructor with specified output file -- print to console and write JSON object to |outFile| */
    public SurveyReader(String JSONFile, String outFile) {
        inFileName = JSONFile;
        outFileName = outFile;
        initArrsAndLists();
        parseJSONFile();
        writeJSONObj(outFile);
    }
    
    /* Write the generated JSON object to |outFile| */
    private void writeJSONObj(String outFile) {
        JSONObject obj = new JSONObject();
        
        /* Initialize the JSON stats object */
        /* obj.put("blah", "blah") */
        
        /* Write to file */
        try (FileWriter file = new FileWriter(outFile)) {
            file.write(obj.toJSONString());
            file.close();
        }
        catch (Exception e) {
            System.out.println("Error opening '" + outFile + "': " + e.toString());
        }
    }
    
    private void initArrsAndLists() {
        /* Arrays */
        movieRatingSums = new double[NUM_MOVIES];
        stdDevRatings = new double[NUM_MOVIES];
        maleRatings = new double[NUM_MOVIES];
        femaleRatings = new double[NUM_MOVIES];
        naRatings = new double[NUM_MOVIES];
        
        /* Lists */
        neStates = Arrays.asList("ME", "NH", "VT", "MA", "RI", "NY", "NJ", "PA", "DE", "MD", "WV", "DC", "CT");
        mwStates = Arrays.asList("OH", "MI", "IN", "IL", "MO", "WI", "MN", "IA", "ND", "SD", "NE", "KS");
        souStates = Arrays.asList("VA", "KY", "TN", "NC", "SC", "GA", "FL", "AL", "MS", "AR", "LA", "TX", "OK");
        westStates = Arrays.asList("NM", "CO", "WY", "MT", "ID", "UT", "NV", "AZ");
        pacStates = Arrays.asList("CA", "OR", "WA", "AK", "HI");
    }
    
    private void parseJSONFile() {
        try {
            JSONParser parser = new JSONParser();
            File f = new File(inFileName);
            Scanner scanner = new Scanner(f);
            
            /* Iterate over the file, line by line (obj by obj) */
            while (scanner.hasNextLine()) {
                String jsonStr = scanner.nextLine();
                
                /* Parse the JSON object */
                Object obj = parser.parse(jsonStr);
                JSONObject jsonObj = (JSONObject)(obj);
                
                /* Get attributes */
                JSONObject respObj = (JSONObject)(jsonObj.get("respondent"));
                JSONArray ratings = (JSONArray)(jsonObj.get("ratings"));
             
                /* Parse the user's info + ratings */
                parseRespondentAndRatings(respObj, ratings);
                
                numResps++;   
            }
        }
        catch (Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        
    }
    
    private void parseRespondentAndRatings(JSONObject resp, JSONArray ratings) {
        String gender = (String) resp.get("gender");
        String state = (String) resp.get("state");
        Integer age = ((Long)resp.get("age")).intValue();
        Integer inc = ((Long)resp.get("income")).intValue();
        Integer edu = ((Long)resp.get("education")).intValue();
        
        /* Gender */
        if (gender.equals("M")) {
            numMales++;
        }
        else if (gender.equals("F")) {
            numFemales++;
        }
        else {/* N/A response */
            numNa++;
        }
        
        /* Age */
        if (age < 20) {
            numUnder20++;
        }
        else if (age < 30) {
            num20To29++;
        }
        else if (age < 40) {
            num30To39++;
        }
        else if (age < 50) {
            num40To49++;
        }
        else if (age < 60) {
            num50To59++;
        }
        else if (age < 70) {
            num60To69++;
        }
        else {
            numOver70++;
        }
        
        /* Income group */
        if (inc == 0) {
            numInc0++;
        }
        else if (inc == 1) {
            numInc1++;
        }
        else if (inc == 2) {
            numInc2++;
        }
        else if (inc == 3) {
            numInc3++;
        }
        else if (inc == 4) {
            numInc4++;
        }
        else {
            numInc5++;
        }
        
        /* Eduction level */
        if (edu == 0) {
            numEdu0++;
        }
        else if (edu == 1) {
            numEdu1++;
        }
        else if (edu == 2) {
            numEdu2++;
        }
        else if (edu == 3) {
            numEdu3++;
        }
        else if (edu == 4) {
            numEdu4++;
        }
        else {
            numEdu5++;
        }
        
        /* Geographic distribution */
        if (neStates.contains(state)) {
            numNe++;
        }
        else if (mwStates.contains(state)) {
            numMw++;
        }
        else if (souStates.contains(state)) {
            numSouth++;
        }
        else if (westStates.contains(state)) {
            numWest++;
        }
        else if (pacStates.contains(state)) {
            numPac++;
        }
        else {
            System.out.println("ERROR: Unrecognized state '" + state + "' found");
            System.exit(1);
        }
        
        parseRatings(ratings, gender);
    } 
    
    private void parseRatings(JSONArray ratings, String gender) {
        Iterator ratingsIter = ratings.iterator();
        
        int index = 0;
        double score;
        while (ratingsIter.hasNext()) {
            score = Double.parseDouble((String)ratingsIter.next());
            /* Counts for non zero ratings by gender */
            if (score > 0) {
                numNonZeroRatings++;
                
                if (gender.equals("M")) {
                    maleNonZeroCount++;
                }
                else if (gender.equals("F")) {
                    femaleNonZeroCount++;
                }
                else if (gender.equals("N/A")) {
                    naNonZeroCount++;
                }
            }

            /* Arrays of movie ratings by gender */
            if (gender.equals("M")) {
                maleRatings[index] += score;   
            }
            else if (gender.equals("F")) {
                femaleRatings[index] += score;   
            }
            else if (gender.equals("N/A")) {
                naRatings[index] += score;   
            }
            index++;
        }
    }
}