import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;

public class summaries extends Configured implements Tool {

	public static class summariesMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				JSONObject obj = new JSONObject(value.toString());
				int gameId = obj.getInt("game");
				context.write(new IntWritable(gameId), value);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static class summariesReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private int userId = 0;
		private int movesCount = 0;
		private int regularMovesCount = 0;
		private int specialMovesCount = 0;
		private String outcome = "";
		private int finalScore = 0;
		private double averagePointsPerMove = 0;
		
		private JSONObject summaryObj;
		
		public void reduce(IntWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			
			summaryObj = new JSONObject();
			
			for (Text jsonText : value) {
				try {
					JSONObject obj = new JSONObject(jsonText.toString());
					updateSummaryObject(obj);
				} catch(JSONException e) {
					e.printStackTrace();
				}
			}
			
			try {
				summaryObj.put("user", userId);
				summaryObj.put("moves", movesCount);
				summaryObj.put("regular", regularMovesCount);
				summaryObj.put("special", specialMovesCount);
				summaryObj.put("outcom", outcome);
				summaryObj.put("score", finalScore);
				summaryObj.put("perMove", averagePointsPerMove);
			} catch(JSONException e) {
				e.printStackTrace();
			}
			
			context.write(key, new Text(summaryObj.toString()));
		}
		
		private void updateSummaryObject(JSONObject obj) throws JSONException {
			JSONObject actionObject = obj.getJSONObject("action");
			
			switch(actionObject.getString("actionType")) {
			case "GameStart":
				String userIdString = obj.getString("user").substring(1);
				userId = Integer.valueOf(userIdString).intValue();
				break;
			case "GameEnd":
				outcome = actionObject.getString("gameStatus");
				finalScore = actionObject.getInt("points");
				break;
			case "Move":
				movesCount++;
				regularMovesCount++;
				finalScore = actionObject.getInt("points");
				averagePointsPerMove = (double)finalScore / (double)movesCount;
				break;
			case "SpecialMove":
				movesCount++;
				specialMovesCount++;
				finalScore = actionObject.getInt("points");
				averagePointsPerMove = (double)finalScore / (double)movesCount;
				break;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "json job");

		job.setJarByClass(summaries.class);

		job.setMapperClass(summariesMapper.class);
		job.setReducerClass(summariesReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new summaries(), args);
		System.exit(res);
	}
}
