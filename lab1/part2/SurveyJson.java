/* 
 * Lab 1-2
 * Authors: Holly Haraguchi (hharaguc@calpoly.edu) and Kevin Costello (kmcostel@calpoly.edu)
 * CPE 369, Winter 2016
 */
 
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Random;
import java.text.DecimalFormat;

public class SurveyJson {                     
    /* Large/medium/small states based on population:         
     * https://en.wikipedia.org/wiki/List_of_U.S._states_and_territories_by_population 
     */                         
    private String[] largeStates = {"CA", "TX", "FL", "NY", "IL", "PA", "OH", "GA", "NC",
                                    "MI", "NJ", "VA", "WA", "AZ", "MA", "TN", "IN"};
    private String[] mediumStates = {"MO", "MD", "WI", "CO", "MN", "SC", "AL", "LA", "KY",
                                     "OR", "OK", "CT", "IA", "UT", "MS", "AR", "NV", "KS"};
    private String[] smallStates = {"NM", "NE", "WV", "ID", "NH", "ME", "RI", "MT",
                                    "DE", "SD", "ND", "AK", "DC", "VT", "WY"};
    private ArrayList<String> maleNames;
    private ArrayList<String> femaleNames;
    private ArrayList<String> lastNames;
    
    private int userId;
 
    public SurveyJson() {
        userId = 1;

        femaleNames = new ArrayList<String>();
        maleNames = new ArrayList<String>();
        lastNames = new ArrayList<String>();

        /* Insert message words into a list */
        fillNames(femaleNames, "female-names.txt");
        fillNames(maleNames, "male-names.txt");
        fillNames(lastNames, "lastnames.txt");
    }
    
    /* Returns an initialized MovieSurvey object */
    public JSONObject genObject() {
        JSONObject userObj = new JSONObject();
        JSONObject resp = new JSONObject();
    
        int age = getAge();
        JSONArray ratings = getRatings(age);
        
        /* Initialize the respondent object */
        resp.put("name", getName());
        resp.put("gender", getGender());
        resp.put("age", getAge());
        resp.put("state", getState());
        resp.put("education", getEducation(age));
        resp.put("income", getIncome(age));
        
        /* Initialize the user object */
        userObj.put("RID", getId());
        userObj.put("respondent", resp);
        userObj.put("ratings", ratings);
        
        return userObj;
    }
    
    private void fillNames(ArrayList<String> list, String fileName) {
        try {
            Scanner s = new Scanner(new File(fileName));
            while (s.hasNext()) {
               list.add(s.next());
            }
            s.close();
        }  
        catch (FileNotFoundException e) {
        System.out.println(fileName + " not found\n");
        }
    }
    
    /* JSON value generators/getters */
    private int getId() {
        return userId++;
    }
    
    /* Full names */
    private String getLastName() {
        Random rand = new Random();
        return lastNames.get(rand.nextInt(lastNames.size()));
    }
    
    private String getFemaleName() {
        Random rand = new Random();
        return femaleNames.get(rand.nextInt(femaleNames.size()));
    }
    
    private String getMaleName() {
        Random rand = new Random();
        return maleNames.get(rand.nextInt(maleNames.size()));
    }
    
    /* States will be categorized into 3 categories.
     * Large, medium, and small. Based on population of the state
     */
    private String getState() {
        double rand = Math.random();
        Random r = new Random();
        int index;
        String state = "";
        
        if (rand < 0.67) { /* large state */
            index = r.nextInt(largeStates.length);
            state = largeStates[index];
        }
        else if (rand < 0.93) { /* medium state */
            index = r.nextInt(mediumStates.length);
            state = mediumStates[index];
        }
        else { /* small state */
            index = r.nextInt(smallStates.length);
            state = smallStates[index];
        }
        
        return state;
    }
    
    /* Ages range from 16 - 85 */
    private int getEducation(int age) {
        double rand = Math.random();
        int educ = 0;
        
        if (age < 18) {
            if (rand < 0.3) {
                educ = 0;
            }
            else {
                educ = 1;
            }
        }
        else if (age < 21) {
            if (rand < 0.2) {
                educ = 0;
            }
            else if (rand < 0.5) {
                educ = 1;
            }
            else if (rand < 0.7) {
                educ = 2;
            }
            else {
                educ = 3;
            }
        }
        else {
            if (rand < 0.2) {
                educ = 0;
            }
            else if (rand < 0.3) {
                educ = 1;
            }
            else if (rand < 0.6) {
                educ = 2;
            }
            else if (rand < 0.88) {
                educ = 3;
            }
            else if (rand < 0.98) {
                educ = 4;
            }
            else {
                educ = 5;
            }
        }
    
        return educ;
    }
    /* Values range from 0 - 5; refer to handout for legend */
    private int getIncome(int age) {
        Random r = new Random();
        double rand = Math.random();
        int income = 0;
        
        if (age < 18) {
            if (rand < 0.95) {
                income = 0;
            }
            else {
                income = 1;
            }
        }
        else if (age >= 18 && age <= 21) {
            /* mostly in categories zero through three */
            if (rand < 0.23) {
                income = 0;
            
            }
            else if (rand < 0.46) {
                income = 1;
            }
            else if (rand < 0.69) {
                income = 2;
            }
            else if (rand < 0.92) {
                income = 3;
            }
            else if (rand < 0.96) {
                income = 4;
            }
            else {
                income = 5;
            }
        }
        else {
            income = r.nextInt(6);
        }
        
        return income;
    }
    
    private JSONObject getName() {
        String gender = getGender();
        String firstName = "";
        String lastName = getLastName();
        double rand = Math.random();
        JSONObject nameObj = new JSONObject();
        
        if (gender.equals("M")) {
            firstName = getMaleName();
        }
        else if (gender.equals("F")) {
            firstName = getFemaleName();
        }
        else {
            if (rand < 0.5) { /* Male */
                firstName = getMaleName();
            }
            else { /* Female */
                firstName = getFemaleName();
            }
        }
        nameObj.put("first", firstName.substring(0,1) + firstName.substring(1).toLowerCase());
        nameObj.put("last", lastName.substring(0,1) + lastName.substring(1).toLowerCase());
        
        return nameObj;
    }
    
    /* Generates a gender, approx. 50/50 split among M/F responses */
    private String getGender() {
        double rand = Math.random();
        
        if (rand < 0.45) {
            return "F";
        }
        else if (rand < 0.9) {
            return "M";
        }
        /* Implicit else */
        return "N/A";
    }
    
    /* Generate a random age */
    /* Most participants should be 18 - 55 */
    /* Total age range is from 16 - 85 */
    private int getAge() {
        Random rand = new Random();
        double perc = Math.random();
        int age = 16;

        /* Age: 16-17 (inclusive) */
        if (perc < 0.10) {
            /* Call to nextInt returns 0 or 1 */ 
            age = rand.nextInt(2) + 16;       
        }
        /* Ages: 18-55 (inclusive) */
        if (perc < 0.90) {
            /* Call to nextInt returns 0-37 */
            age = rand.nextInt(38) + 18;
        }
        /* Ages: 56-85 (inclusive) */
        else { 
            age = rand.nextInt(30) + 56; 
        }
        
        return age;
    }
    
    private JSONArray getRatings(int age) {
        JSONArray ratings = new JSONArray();
        Random rand = new Random(); /* used to generate scores */
        double chance = Math.random(); /* used to determine the probability a movie has not been seen */
        DecimalFormat df = new DecimalFormat("#.#");
        double hasAll = Math.random();
        
        /* Star Wars; nextInt returns 0 - 4 inclusive
         * People who like Star Wars tend to like Batman Begins, Avatar, Suicide Squad.
         */
        double starScore = Math.random() + rand.nextInt(5) + 5;
        if (chance < 0.005 && hasAll < 0.4) {
            starScore = 0;
        }
        
        /* God Father; influences Rocky score */
        chance = Math.random();
        double godScore = rand.nextInt(10) + chance;  
        if (chance < 0.06 && hasAll < 0.4) {
            godScore = 0;
        }
    
        /* Batman Begins; Influences Memento */
        chance = Math.random();
        double batScore = rand.nextInt(10) + chance;
        if (chance < 0.00001 && hasAll < 0.4) {
            batScore = 0;
        }
        
        /* Memento; depends on Batman's score */
        chance = Math.random();
        double memScore = Math.floor(batScore) + Math.random();
        if (chance < 0.009 && hasAll < 0.4) {
            memScore = 0;
        }
        
        /* Rocky; influenced by God Father's score */
        double rockyScore = Math.floor(godScore) + Math.random();
        chance = Math.random();
        if (chance < 0.013 && hasAll < 0.4) {
            rockyScore = 0;
        }
        
        /* Princess Bride; ages 35 - 55 mostly like this movie*/
        chance = Math.random();
        double prinScore = 5;
        if (age >= 35 && age <= 55) {
            prinScore = rand.nextInt(4) + 6 + Math.random();
        }
        else if (chance <= 0.45) {
            prinScore = rand.nextInt(5) + 1 + Math.random();
        }
        else if (chance <= 0.95) {
            prinScore = rand.nextInt(5) + 3 + Math.random();
        }
        
        if (hasAll < 0.4) {
            prinScore = 0;
        }
        
        /* Sleepless in Seattle */
        chance = Math.random();
        double sleepScore = rand.nextInt(10) + Math.random();
        if (chance < 0.09 && hasAll < 0.4) {
            sleepScore = 0;
        }
        
        /* Pretty Woman */
        chance = Math.random();
        double prettyScore = rand.nextInt(10) + Math.random();
        if (chance < 0.12 && hasAll < 0.4) {
            prettyScore = 0;
        }
        
        /* Saw; either really liked or disliked
         * Inversely related with Pretty Woman, Princess Bride, and Sleepless Seattle 
         */
        double romAvg = (prettyScore + prinScore + sleepScore) / 3;
        double sawScore = 0;
        if (romAvg >= 1.5 && romAvg <= 5.0) {
            sawScore = rand.nextInt(4) + 6 + Math.random();
        }
        else if (romAvg <= 10) {
            sawScore = rand.nextInt(4) + Math.random();
        }
        
        /* Avatar; influenced by Star Wars' score*/
        chance = Math.random();
        double avaScore = Math.floor(starScore) + Math.random(); /* Make this more varied? */
        if (chance < 0.08 && hasAll < 0.4) {
            avaScore = 0;
        }
        
        /* Dogma; either really liked or disliked */
        chance = Math.random();
        /* Default to good rating */
        double dogScore = rand.nextInt(4) + 6 + Math.random();
       
        if (chance <= 0.5) {
            dogScore = rand.nextInt(4) + Math.random();
        }
        else if (hasAll < 0.4) {
            dogScore = 0;
        }
        
        /* Suicide Squad; influenced by Star Wars' score*/
        chance = Math.random();
        double suicScore = Math.floor(starScore) + Math.random();
        if (chance < 0.05 && hasAll < 0.4) {
            suicScore = 0;
        }
        
        /* Beverly Hills Cop; either really liked or disliked */
        chance = Math.random();
        /* Default to good rating */
        
        double bevScore = rand.nextInt(4) + 6 + Math.random();
        
        if (chance <= 0.5) {
            bevScore = rand.nextInt(4) + Math.random();
        }
        else if (hasAll < 0.4) {
            bevScore = 0;
        }
        
        /* Add the ratings to the JSONArray */
        ratings.add(df.format(starScore));
        ratings.add(df.format(godScore));
        ratings.add(df.format(memScore));
        ratings.add(df.format(sawScore));
        ratings.add(df.format(rockyScore));
        ratings.add(df.format(prinScore));
        ratings.add(df.format(sleepScore));
        ratings.add(df.format(prettyScore));
        ratings.add(df.format(avaScore));
        ratings.add(df.format(dogScore));
        ratings.add(df.format(batScore));
        ratings.add(df.format(suicScore));
        ratings.add(df.format(bevScore));
        
        return ratings;
    }
}