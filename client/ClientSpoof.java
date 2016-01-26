import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.json.*;
import org.bson.Document;

public class ClientSpoof {
   private static ArrayList<String> dictionary = new ArrayList<String>();
   
   public static void main(String[] args) throws JSONException, InterruptedException, FileNotFoundException {
      Logger logger = Logger.getLogger("org.mongodb.driver");  // turn off logging
      logger.setLevel(Level.OFF);
      
      String configName = null;
      if (args.length == 1) {
         configName = args[0];
      }
      else {
         System.out.println("Incorrect usage! java Client <config-filename>");
         return;
      }
      JSONObject config = null;
      try {
         config = readConfig(configName);
      } catch (JSONException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      if (config == null) {
         return;
      }
      PrintStream logfile = new PrintStream(new File(config.getString("clientLog")));
      
      readDictionary(config.getString("words"));
      
      MongoClient c = new MongoClient(config.getString("mongo"));
      MongoDatabase db = c.getDatabase(config.getString("database"));
      
      MongoCollection<Document> dox = db.getCollection(config.getString("collection"));
      
      String meta = "";
      
      Date d = new Date();
      Timestamp ts = new Timestamp(d.getTime());
      meta += ts.toString() + '\n';
      //System.out.println(ts);
      //logfile.println(ts);
      
      //System.out.println("Connected to Mongo Server at " + config.getString("mongo") + " on port " + config.getInt("port"));
      meta += "Connected to Mongo Server at " + config.getString("mongo") + " on port " + config.getInt("port") + '\n';
      //System.out.println("Using database '" + config.getString("database") + "' and collection '" + config.get("collection") + "'.");
      meta += "Using database '" + config.getString("database") + "' and collection '" + config.get("collection") + "'." + '\n';
      //System.out.println("Number of documents in collection: " + dox.count());
      meta += "Number of documents in collection: " + dox.count() + '\n';
      
      System.out.println(meta);
      logfile.println(meta);
      
      Random rand = new Random();
      int delay = config.getInt("delay");
      int stdev = delay / 2;
      System.out.println("delay: " + delay);
      System.out.println("stdev: " + stdev);
      int i = 1;
      while (true) {
         String output = "";
         int sleep = (int)(rand.nextGaussian() * stdev + delay);
         System.out.println("Sleeping for: " + sleep + " seconds.");
         TimeUnit.SECONDS.sleep(sleep);
         Message m = new Message(i, genText());
         
         dox.insertOne(m.toDocument());
         output += new Timestamp(d.getTime()) + ": " + '\n' ;
         //System.out.println(new Timestamp(d.getTime()) + ": ");
         output += m.toString(3) + '\n';
         //System.out.println(m.toString(3));
         if (i++ % 2 == 0) {
            output += "**************" + '\n';
            //System.out.println("**************");
            //System.out.println("Total number of messages stored: " + dox.count());
            output += "Total number of messages stored: " + dox.count() + '\n';
            Document filter = new Document();
            filter.put("user", m.user);
            //System.out.println("Total number of messages written by last auther: " + dox.count(filter));
            output += "Total number of messages written by last auther: " + dox.count(filter) + '\n';
            //System.out.println("**************");
            output += "**************";
         }
         else {
            //System.out.println("*******");
            output += "*******";
         }
         logfile.println(output);
         System.out.println(output);
         output = "";
      }
      
      /*
      FindIterable<Document> result = dox.find();
      result.forEach(new Block<Document>() {        // print each retrieved document
         public void apply(final Document d) {
             //System.out.println(d);
         }
      });
      */
   }
   
   private static JSONObject readConfig(String configName) throws JSONException {
      File configFile = new File(configName);
      FileInputStream fis = null;
      try {
         fis = new FileInputStream(configFile);
      } catch (FileNotFoundException e) {
         System.out.println("Failed to open file! Exiting.");
         e.printStackTrace();
         return null;
      }
      byte[] data = new byte[(int) configFile.length()];
      JSONObject configJson = null;
      String configStr = null;
      try {
         fis.read(data);
         fis.close();
         configStr = new String(data, "UTF-8");
         configJson = new JSONObject(configStr);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         return null;
      } catch (JSONException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         return null;
      }
      if (!configJson.has("mongo")) {
         configJson.put("mongo", "localhost");
      }
      if (!configJson.has("port")) 
         configJson.put("port", 27107);
      if (!configJson.has("database")) 
         configJson.put("database", "test");
      if (configJson.get("collection").equals(configJson.get("monitor"))) {
         System.out.println("Collection and monitor names are same!");
         return null;
      }
      if (!configJson.has("delay")) 
         configJson.put("delay", 10);
      
      int delayAmount = configJson.getInt("delay");
      if (delayAmount == 0)
         configJson.put("delay", 10);
      
      if (!configJson.has("words")) {
         System.out.println("Missing words file!");
         return null;
      }

      if (!configJson.has("clientLog")) {
         System.out.println("Missing clientLog file!");
         return null;
      }

      if (!configJson.has("serverLog")) {
         System.out.println("Missing serverLog file!");
         return null;
      }

      if (!configJson.has("wordFilter")) {
         System.out.println("Missing wordFilter file!");
         return null;
      }
      

      System.out.println(configJson.toString(4));
      return configJson;
   }   
   
   private static void readDictionary(String filename) {
      FileReader fr = null;
      try {
         fr = new FileReader(filename);
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      }
      BufferedReader br = new BufferedReader(fr);
      
      String line = null;
      try {
         while ((line = br.readLine()) != null) {
            dictionary.add(line);
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
   }
   
   private static String genText() {
      String text = "";
      Random rand = new Random();
      int numWords = rand.nextInt(19) + 2;
      for (int i = 0; i < numWords; i++) {
         int wordIdx = rand.nextInt(dictionary.size());
         text += dictionary.get(wordIdx) + " ";
      }
      
      return text;
   }
}
