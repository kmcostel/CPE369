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
import java.text.NumberFormat;
 
public class SurveyReader {
    /* MR4 - Population Statistics */
    /* Survey respondents */
    private int numResps;
    private int numFemales;
    private int numMales;
    private int numNa;
    
    /* Age distribution */
    private int numUnder20;
    private int num20s;
    private int num30s;
    private int num40s;
    private int num50s;
    private int num60s;
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
    
    /* ArrayList to hold all ratings for each movie */
    ArrayList<Double> starRatings;
    ArrayList<Double> godRatings;
    ArrayList<Double> memRatings;
    ArrayList<Double> sawRatings;
    ArrayList<Double> rockyRatings;
    ArrayList<Double> princessRatings;
    ArrayList<Double> sleepRatings;
    ArrayList<Double> prettyRatings;
    ArrayList<Double> avatarRatings;
    ArrayList<Double> dogRatings;
    ArrayList<Double> batRatings;
    ArrayList<Double> suicideRatings;
    ArrayList<Double> beverleyRatings;
    
    /* Movie averages */
    private double starAvg;
    private double godAvg;
    private double memAvg;
    private double sawAvg;
    private double rockyAvg;
    private double princessAvg;
    private double sleepAvg;
    private double prettyAvg;
    private double avatarAvg;
    private double dogAvg;
    private double batAvg;
    private double suicideAvg;
    private double beverleyAvg;
    
    /* Movie standard deviations */
    private double starStdDev;
    private double godStdDev;
    private double memStdDev;
    private double sawStdDev;
    private double rockyStdDev;
    private double princessStdDev;
    private double sleepStdDev;
    private double prettyStdDev;
    private double avatarStdDev;
    private double dogStdDev;
    private double batStdDev;
    private double suicideStdDev;
    private double beverleyStdDev;

    /* MR6 - Movie ratings by gender */
    /* total number of nonzero movie ratings by gender value */
    int maleNonZeroCount;
    int femaleNonZeroCount;
    int naNonZeroCount;
    
    /* Used to calculate movie rating average for each gender value; these arrays will hold sums */
    double[] maleRatings;
    double[] femaleRatings;
    double[] naRatings;
    
    /* Tracks non-zero ratings for each movie for each gender */
    double[] maleRatingCounts;
    double[] femaleRatingCounts;
    double[] naRatingCounts;
    
    private List<String> movieNames;

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
        printStats();
    }
   
   /* Constructor with specified output file -- print to console and write JSON object to |outFile| */
    public SurveyReader(String JSONFile, String outFile) {
        inFileName = JSONFile;
        outFileName = outFile;
        initArrsAndLists();
        parseJSONFile();
        printStats();
        writeJSONObj(outFile);
    }
    
    private void printStats() {
        NumberFormat numFormat = NumberFormat.getNumberInstance();
        numFormat.setMinimumFractionDigits(3);
        
        /* Movie averages calculation */
        starAvg = getAverage(starRatings);
        godAvg = getAverage(godRatings);
        memAvg = getAverage(memRatings);
        sawAvg = getAverage(sawRatings);
        rockyAvg = getAverage(rockyRatings);
        princessAvg = getAverage(princessRatings);
        sleepAvg = getAverage(sleepRatings);
        prettyAvg = getAverage(prettyRatings);
        avatarAvg = getAverage(avatarRatings);
        dogAvg = getAverage(dogRatings);
        batAvg = getAverage(batRatings);
        suicideAvg = getAverage(suicideRatings);
        beverleyAvg = getAverage(beverleyRatings);
        
        /* Movie standard deviation calculation */
        starStdDev = calcStdDev(starRatings, starAvg);
        godStdDev = calcStdDev(godRatings, godAvg);
        memStdDev = calcStdDev(memRatings, memAvg);
        sawStdDev = calcStdDev(sawRatings, sawAvg);
        rockyStdDev = calcStdDev(rockyRatings, rockyAvg);
        princessStdDev = calcStdDev(princessRatings, princessAvg);
        sleepStdDev = calcStdDev(sleepRatings, sleepAvg);
        prettyStdDev = calcStdDev(prettyRatings, prettyAvg);
        avatarStdDev = calcStdDev(avatarRatings, avatarAvg);
        dogStdDev = calcStdDev(dogRatings, dogAvg);
        batStdDev = calcStdDev(batRatings, batAvg);
        suicideStdDev = calcStdDev(suicideRatings, suicideAvg);
        beverleyStdDev = calcStdDev(beverleyRatings, beverleyAvg);
    
        System.out.println();
        /* MR4 - Population Statistics */
        System.out.println("Total number of survey respondents: " + numResps);
        System.out.println("\nNumber of male respondents out of total: " + numMales + "/" + numResps);
        System.out.println("Percentage of respondents who are male: " + numFormat.format((double)numMales / numResps));
        
        System.out.println("\nNumber of female respondents out of total: " + numFemales +"/"+numResps);
        System.out.println("Percentage of respondents who are female: " + numFormat.format( (double)numFemales / numResps));
        
        System.out.println("\nNumber of 'N/A' respondents out of total: " + numNa +"/"+numResps);
        System.out.println("Percentage of respondents who answered 'N/A': " + numFormat.format( (double)numNa / numResps));

        /* Distribution of survey respondents by age groups */
        System.out.println("\nDistribution of respondents by age group:");
        System.out.println("  - Younger than 20: " + numUnder20);
        System.out.println("  - Between 20 and 29: " + num20s);
        System.out.println("  - Between 30 and 39: " + num30s);
        System.out.println("  - Between 40 and 49: " + num40s);
        System.out.println("  - Between 50 and 59: " + num50s);
        System.out.println("  - Between 60 and 69: " + num60s);
        System.out.println("  - Older than 70: " + numOver70);
        
        /* Distribution of survey respondents by income group */
        System.out.println("\nDistribution of respondents by income group:");
        System.out.println("  - No independent income: " + numInc0);
        System.out.println("  - $0 --- $20,000: " + numInc1);
        System.out.println("  - $20,000 --- $40,000: " + numInc2);
        System.out.println("  - $40,000 --- $60,000: " + numInc3);
        System.out.println("  - $60,000 --- $100,000: " + numInc4);
        System.out.println("  - $100,000+ per year: " + numInc5);
        
        /* Distribution of survey respondents by education levels */
        System.out.println("\nDistribution of respondents by education level:");
        System.out.println("  - Declined to state: " + numEdu0);
        System.out.println("  - Less than high school: " + numEdu1);
        System.out.println("  - High school graduate: " + numEdu2);
        System.out.println("  - Some college: " + numEdu3);
        System.out.println("  - BS degree: " + numEdu4);
        System.out.println("  - Advanced degree (MS, Ph.D., JD, MD, etc.) : " + numEdu5);
        
        /* Geographic distribution */
        System.out.println("\nDistribution of respondents by geographic region:");
        System.out.println("  - Northeast: " + numNe);
        System.out.println("  - Midwest: " + numMw);
        System.out.println("  - South: " + numSouth);
        System.out.println("  - West: " + numWest);
        System.out.println("  - Pacific West: " + numPac);

        /* MR5 - Movie Ratings */
        /* ArrayLists of movie ratings hold scores of value zero */
        System.out.println("\nTotal number of non-zero ratings: " + numNonZeroRatings);
        System.out.println("\nAverage rating of each movie:");
        System.out.println("  - Star Wars: A New Hope: " + starAvg);
        System.out.println("  - Godfather: " + godAvg);
        System.out.println("  - Memento: " + memAvg);
        System.out.println("  - Saw: " + sawAvg);
        System.out.println("  - Rocky: " + rockyAvg);
        System.out.println("  - Princess Bride: " + princessAvg);
        System.out.println("  - Sleepless in Seattle: " + sleepAvg);
        System.out.println("  - Pretty Woman: " + prettyAvg);
        System.out.println("  - Avatar: " + avatarAvg);
        System.out.println("  - Dogma: " + dogAvg);
        System.out.println("  - Batman Begins: " + batAvg);
        System.out.println("  - Suicide Squad: " + suicideAvg);
        System.out.println("  - Beverley Hells Cop: " + beverleyAvg);
        
        /* Movie standard deviations */
        System.out.println("\nStandard deviation for each movie's ratings:");
        System.out.println("  - Star Wars: A New Hope: " + starStdDev);
        System.out.println("  - Godfather: " + godStdDev);
        System.out.println("  - Memento: " + memStdDev);
        System.out.println("  - Saw: " + sawStdDev);
        System.out.println("  - Rocky: " + rockyStdDev);
        System.out.println("  - Princess Bride: " + princessStdDev);
        System.out.println("  - Sleepless in Seattle: " + sleepStdDev);
        System.out.println("  - Pretty Woman: " + prettyStdDev);
        System.out.println("  - Avatar: " + avatarStdDev);
        System.out.println("  - Dogma: " + dogStdDev);
        System.out.println("  - Batman Begins: " + batStdDev);
        System.out.println("  - Suicide Squad: " + suicideStdDev);
        System.out.println("  - Beverley Hells Cop: " + beverleyStdDev);
       
        /* MR6 - Movie Ratings by Gender (for each movie) */
        System.out.println("\nTotal number of non-zero ratings by gender:");
        System.out.println("  - Male: " + maleNonZeroCount);
        System.out.println("  - Female: " + femaleNonZeroCount);
        System.out.println("  - N/A responses: " + naNonZeroCount);

        System.out.println("\nTotal number of non-zero ratings by gender:");
        for (int i = 0; i < NUM_MOVIES; i++) {
            System.out.println("  -" + movieNames.get(i) + ":");
            System.out.println("    - Males: " + maleRatingCounts[i]);
            System.out.println("    - Females: " + femaleRatingCounts[i]);
            System.out.println("    - N/A responses: " + naRatingCounts[i]);
        }

        System.out.println("\nAverage movie rating by gender:"); // should we divide by numMales?
        for (int i = 0; i < NUM_MOVIES; i++) {
            System.out.println("  -" + movieNames.get(i) + ":");
            System.out.println("    - Males: " + (maleRatings[i] / numMales > 0 ? maleRatings[i] / numMales : 0)); //numMales?
            System.out.println("    - Females: " + (femaleRatings[i] / numFemales > 0 ? femaleRatings[i] / numFemales : 0)); //numFemales?
            System.out.println("    - N/A responses: " + (naRatings[i] / numNa > 0 ? naRatings[i] / numNa : 0));
        }
    }
    
    /* Write the generated JSON object to |outFile| */
    private void writeJSONObj(String outFile) {
        JSONObject obj = new JSONObject();
        
        /* Initialize the JSON stats object */

        /* MR4 - Population Statistics */
        obj.put("totalResp", numResps);
        obj.put("numFemales", numFemales);
        obj.put("numMales", numMales);
        obj.put("numNa", numNa);
        obj.put("percFemales", ((double)numFemales) / numResps);
        obj.put("percMales", ((double)numMales) / numResps);
        obj.put("percNa", ((double)numNa) / numResps);

        obj.put("numUnder20", numUnder20);
        obj.put("num20s", num20s);
        obj.put("num30s", num30s);
        obj.put("num40s", num40s);
        obj.put("num50s", num50s);
        obj.put("num60s", num60s);
        obj.put("numOver70", numOver70);

        obj.put("numInc0", numInc0);
        obj.put("numInc1", numInc1);
        obj.put("numInc2", numInc2);
        obj.put("numInc3", numInc3);
        obj.put("numInc4", numInc4);
        obj.put("numInc5", numInc5);

        obj.put("numEdu0", numEdu0);
        obj.put("numEdu1", numEdu1);
        obj.put("numEdu2", numEdu2);
        obj.put("numEdu3", numEdu3);
        obj.put("numEdu4", numEdu4);
        obj.put("numEdu5", numEdu5);

        obj.put("numNe", numNe);
        obj.put("numMw", numMw);
        obj.put("numSouth", numSouth);
        obj.put("numWest", numWest);
        obj.put("numPac", numPac);

        /* MR5 - Movie Ratings */
        obj.put("numNonZeroRatings", numNonZeroRatings);

        obj.put("starWarsAvgRating", starAvg);
        obj.put("godfatherAvgRating", godAvg);
        obj.put("mementoAvgRating", memAvg);
        obj.put("sawAvgRating", sawAvg);
        obj.put("rockyAvgRating", rockyAvg);
        obj.put("princessBrideAvgRating", princessAvg);
        obj.put("sleeplessSeattleAvgRating", sleepAvg);
        obj.put("prettyWomanAvgRating", prettyAvg);
        obj.put("avatarAvgRating", avatarAvg);
        obj.put("dogAvgRating", dogAvg);
        obj.put("batmanBeginsAvgRating", batAvg);
        obj.put("suicideSquadAvgRating", suicideAvg);
        obj.put("beverleyHillCopAvgRating", beverleyAvg);
       
        /* ST DEV GOES HERE */
        obj.put("starWarsStdDev", starStdDev);
        obj.put("godfatherStdDev", godStdDev);
        obj.put("mementoStdDev", memAvg);
        obj.put("sawStdDev", sawStdDev);
        obj.put("rockyStdDev", rockyStdDev);
        obj.put("princessBrideStdDev", princessStdDev);
        obj.put("sleeplessSeattleStdDev", sleepStdDev);
        obj.put("prettyWomanStdDev", prettyStdDev);
        obj.put("avatarStdDev", avatarStdDev);
        obj.put("dogmaStdDev", dogStdDev);
        obj.put("batmanBeginsStdDev", batStdDev);
        obj.put("suicideSquadStdDev", suicideStdDev);
        obj.put("beverleyHillsCopStdDev", beverleyStdDev);

        /* MR6 - Ratings by Gender */
        obj.put("maleNonZeroCount", maleNonZeroCount);
        obj.put("femaleNonZeroCount", femaleNonZeroCount);
        obj.put("naNonZeroCount", naNonZeroCount);

        /* Males */
        for (int i = 0; i < NUM_MOVIES; i++) {
            if (maleNonZeroCount > 0) {
                obj.put("avgMaleRatingMov" + i, maleRatings[i] / maleNonZeroCount);  
            }
            else {
                obj.put("avgMaleRatingMov" + i, 0);
            }       
        }

        /* Females */
        for (int i = 0; i < NUM_MOVIES; i++) {
            if (femaleNonZeroCount > 0) {
                obj.put("avgFemRatingMov" + i, femaleRatings[i] / femaleNonZeroCount);
            }
            else {
                obj.put("avgFemRatingMov" + i, 0);
            }
        }
            
        /* N/A responses */
        for (int i = 0; i < NUM_MOVIES; i++) {
            if (naNonZeroCount > 0) {
                obj.put("avgNaRatingMov" + i, naRatings[i] / naNonZeroCount);
            }
            else {
                obj.put("avgNaRatingMov" + i, 0); 
            }
        }
        
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
        maleRatings = new double[NUM_MOVIES];
        femaleRatings = new double[NUM_MOVIES];
        naRatings = new double[NUM_MOVIES];
        
        maleRatingCounts = new double[NUM_MOVIES];
        femaleRatingCounts = new double[NUM_MOVIES];
        naRatingCounts = new double[NUM_MOVIES];
        
        /* Lists */
        neStates = Arrays.asList("ME", "NH", "VT", "MA", "RI", "NY", "NJ", "PA", "DE", "MD", "WV", "DC", "CT");
        mwStates = Arrays.asList("OH", "MI", "IN", "IL", "MO", "WI", "MN", "IA", "ND", "SD", "NE", "KS");
        souStates = Arrays.asList("VA", "KY", "TN", "NC", "SC", "GA", "FL", "AL", "MS", "AR", "LA", "TX", "OK");
        westStates = Arrays.asList("NM", "CO", "WY", "MT", "ID", "UT", "NV", "AZ");
        pacStates = Arrays.asList("CA", "OR", "WA", "AK", "HI");
        movieNames = Arrays.asList("Star Wars: A New Hope", "Godfather", "Memento", "Saw", "Rocky",
                                   "Princess Bride", "Sleepless in Seattle", "Pretty Woman", "Avatar",
                                   "Dogma", "Batman Begins", "Suicide Squad", "Beverly Hills Cop");
    
        starRatings = new ArrayList<Double>();
        godRatings = new ArrayList<Double>();
        memRatings = new ArrayList<Double>();
        sawRatings = new ArrayList<Double>();
        rockyRatings = new ArrayList<Double>();
        princessRatings = new ArrayList<Double>();
        sleepRatings = new ArrayList<Double>();
        prettyRatings = new ArrayList<Double>();
        avatarRatings = new ArrayList<Double>();
        dogRatings = new ArrayList<Double>();
        batRatings = new ArrayList<Double>();
        suicideRatings = new ArrayList<Double>();
        beverleyRatings = new ArrayList<Double>();
    
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
            System.out.println("ERROR: " + e.toString());
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
            num20s++;
        }
        else if (age < 40) {
            num30s++;
        }
        else if (age < 50) {
            num40s++;
        }
        else if (age < 60) {
            num50s++;
        }
        else if (age < 70) {
            num60s++;
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
                    maleRatingCounts[index] =  maleRatingCounts[index] + 1;
                }
                else if (gender.equals("F")) {
                    femaleNonZeroCount++;
                    femaleRatingCounts[index] = femaleRatingCounts[index] + 1;
                }
                else if (gender.equals("N/A")) {
                    naNonZeroCount++;
                    naRatingCounts[index] = naRatingCounts[index] + 1;
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

            /* Add movie rating to the movie's arraylist of scores */
            addMovieScore(score, index);

            index++;
        }
    }
    
    private double getAverage(ArrayList<Double> list) {
        double score = 0.0;
        int size = list.size();
        
        for (int i = 0; i < size; i++) {
            score += list.get(i);
        }
        
        return score / size > 0 ? score / size : 0;
    }
    
    private double calcStdDev(ArrayList<Double> data, double mean) {
        double temp = 0;
      
        for (double d : data) {
            temp += (d-mean) * (d-mean);         
        }
      
        return Math.sqrt(temp / data.size());
    }
    
    private void addMovieScore(double score, int index) {
        
        if (index == 0) {
            starRatings.add(score);
        }
        else if (index == 1) {
            godRatings.add(score);
        }
        else if (index == 2) {
            memRatings.add(score);
        }
        else if (index == 3) {
            sawRatings.add(score);
        }
        else if (index == 4) {
            rockyRatings.add(score);
        }
        else if (index == 5) {
            princessRatings.add(score);
        }
        else if (index == 6) {
            sleepRatings.add(score);
        }
        else if (index == 7) {
            prettyRatings.add(score);
        }
        else if (index == 8) {
            avatarRatings.add(score);
        }
        else if (index == 9) {
            dogRatings.add(score);
        }
        else if (index == 10) {
            batRatings.add(score);
        }
        else if (index == 11) {
            suicideRatings.add(score);
        }
        else if (index == 12) {
            beverleyRatings.add(score);
        }
    }
}