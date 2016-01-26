import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;
import java.util.logging.Level;
/**
 * 
 * @author Pak Long (Francis) Yuen
 */
public class Server {
	
	private static final String kDefaultPortNum = "27017";

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Wrong Usage: java -jar server.jar <config file name>");
			return;
		}
		
		// 1. Read JSON configuration file
		JSONTokener tokenizer = null;
		JSONObject config = null;
		try {
			tokenizer = new JSONTokener(new FileReader(new File(args[0])));
		} catch (FileNotFoundException e) {
			System.out.println("<Error: Config file not found.>");
			return;
		}
		try {
			tokenizer.skipTo('{');
			config = new JSONObject(tokenizer);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		// Turn off MongoDB Logger
		Logger logger = Logger.getLogger("org.mongodb.driver");
		logger.setLevel(Level.OFF);                              
		
		// 2. Connect to MongoDB
		// Connect to Client
		String connection = "";
		try {
			String host = config.getString("mongo");
			if (host == null || host.isEmpty())
				connection = "localhost";
			else
				connection = host;
			
			String port = config.getString("port");
			if (port == null || port.isEmpty())
				connection += ":" + kDefaultPortNum;
			else
				connection += ":" + port;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		MongoClient client = new MongoClient(connection);
		
		// Connect to Database
		String dbName = "";
		try {
			dbName = config.getString("database");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (dbName == null || dbName.isEmpty())
			dbName = "test";
		MongoDatabase db = client.getDatabase(dbName);
		
		// Connection to Collection
		String colName = "";
		try {
			colName = config.getString("collection");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (colName == null || colName.isEmpty()) {
			System.out.println("<Error: collection name is empty>");
			return;
		}
		MongoCollection<Document> collection = db.getCollection(colName);
		
		// 3. Get the number of objects in the collection
		long numObjs = collection.count();
		
		// 4. Clear monitor collection
		String monitorName = "";
		try {
			monitorName = config.getString("monitor");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (monitorName == null || monitorName.isEmpty()) {
			System.out.println("<Error: monitor collection name is empty>");
			return;
		}
		MongoCollection<Document> monitorCol = db.getCollection(monitorName);
		monitorCol.deleteMany(new Document());
		
		// 5. Read queryWordFile & wordFile
		FileReader queryWordFile = null;
		String queryWordFileName = "";
		String wordFileName = "";
		
		ArrayList<String> queryWords = new ArrayList<String>();
		ArrayList<String> wordsList = new ArrayList<String>();
		
		try {
			queryWordFileName = config.getString("wordFilter");
			wordFileName = config.getString("words");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		try {
			queryWordFile = new FileReader(queryWordFileName);
		} catch (FileNotFoundException e) {
			System.out.println("<Error: Query Word File not found.>");
			return;
		}
		FileReader wordFile = null;
		try {
			wordFile = new FileReader(wordFileName);
		} catch (FileNotFoundException e) {
			System.out.println("<Error: Word File not found.>");
			return;
		}
		BufferedReader queryReader = new BufferedReader(queryWordFile);
		BufferedReader wordReader = new BufferedReader(wordFile);
		
		// Read query words
		String line = null;
	  	try {
	  		while ((line = queryReader.readLine()) != null) {
  				queryWords.add(line.trim());
	  		}
	  	} catch (IOException e) {
	  		e.printStackTrace();
	  	}
	  	// Read word list
	  	try {
	  		while ((line = wordReader.readLine()) != null) {
	  			wordsList.add(line.trim());
	  		}
	  	} catch (IOException e) {
	  		e.printStackTrace();
	  	}
	  	
	  	// 6. Print startup diagnostics
	  	System.out.println("============================================");
	  	System.out.println("     StartUp Diagnostics");
	  	System.out.println("============================================");
	  	System.out.print("Current Time: ");
	  	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	  	Date date = new Date();
	  	System.out.println(dateFormat.format(date));
	  	System.out.println("MongoDB Connection Details");
	  	System.out.println("\tMongoDB Host: " + connection.split(":")[0]);
	  	System.out.println("\tMongoDB Port: " + connection.split(":")[1]);
	  	System.out.println("Database Name: " + dbName);
	  	System.out.println("Collection Name: " + colName);
	  	System.out.println("Monitor Collection Name: " + monitorName);
	  	System.out.printf("Number of documents in collection: %d%n", numObjs);
	
	  	while (true) {
	  		runMainLoop(collection, monitorCol, config);
	  	}
	}
	
	private static void runMainLoop(MongoCollection<Document> col
			, MongoCollection<Document> monitor, JSONObject config) {
		
	}
}









