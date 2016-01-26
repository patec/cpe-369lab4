import java.util.Random;

import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;


public class Message {
   int messageId;
   String user;
   String recepient;
   String status;
   String text;
   int ogMessageId;
   
   public Message(int messageId, String text) {
      Random rand = new Random();
      this.messageId = messageId;
      this.text = text;
      this.user = genUserId();
      int randomInt = rand.nextInt(10);
      if (randomInt < 6) {
         this.status = "public";
         randomInt = rand.nextInt(100);
         if (randomInt < 10) {
            this.recepient = "self";
         }
         else if (randomInt < 20) {
            this.recepient = genUserId(); 
         }
         else if (randomInt < 60) {
            this.recepient = "subscribers";
         }
         else {
            this.recepient = "all";
         }
      }
      else if (randomInt < 8) {
         this.status = "protected";
         randomInt = rand.nextInt(10);
         if (randomInt < 2) {
            this.recepient = "self";
         }
         else if (randomInt < 8) {
            this.recepient = "subscribers";
         }
         else {
            this.recepient = genUserId();
         }
      }
      else {
         this.status = "private";
         randomInt = rand.nextInt(10);
         this.recepient = randomInt > 8 ? "self" : genUserId();
      }
      
      randomInt = rand.nextInt(10);
      if (randomInt < 2) {
         this.ogMessageId = rand.nextInt(messageId);
      }
      else {
         this.ogMessageId = -1;
      }
      
   }
   
   private String genUserId() {
      Random rand = new Random();
      return "u" + new Integer(rand.nextInt(10000) + 1).toString();
   }
   
   public String toString() {
      JSONObject obj = new JSONObject();
      try {
         obj.put("messageId",  this.messageId);
         obj.put("user", this.user);
         obj.put("status", this.status);
         obj.put("recepient",  this.recepient);
         obj.put("text",  this.text);
         if (this.ogMessageId != -1) {
            obj.put("in-response", this.ogMessageId);
         }
      } catch (JSONException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      return obj.toString();
   }
   
   public String toString(int indent) {
      JSONObject obj = new JSONObject();
      try {
         obj.put("messageId",  this.messageId);
         obj.put("user", this.user);
         obj.put("status", this.status);
         obj.put("recepient",  this.recepient);
         obj.put("text",  this.text);
         if (this.ogMessageId != -1) {
            obj.put("in-response", this.ogMessageId);
         }
         return obj.toString(indent);
      } catch (JSONException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      return null;
   }
   
   public Document toDocument() {
      Document d = new Document();
      d.put("recepient", this.recepient);
      d.put("messageId", this.messageId);
      d.put("in-response", this.ogMessageId);
      d.put("status", this.status);
      d.put("text", this.text);
      d.put("user", this.user);
      return d;
   }
   
}
