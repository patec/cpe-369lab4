import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text; // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path; // Hadoop's implementation of directory path/filename
import org.apache.hadoop.util.Tool;
import org.json.JSONException;
import org.json.JSONObject;

public class activity {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res1 = ToolRunner.run(conf, new activityOne(), args);
		int res2 = ToolRunner.run(conf, new activityTwo(), args);
		System.exit(res1 == res2 ? res1 : 0);
	}
}

/**
 * Outputs: Key: userId Value: { status: <"Win" or "Lose">, score: <final
 * score>, moves: <number of moves> }
 */
class activityOne extends Configured implements Tool {
	public static class activityOneMapper extends
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

	public static class activityOneReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			try {
				int nMoves = 0;
				String endStatus = "";
				int score = 0;
				int userId = 0;
				boolean didEnd = false;

				for (Text value : values) {
					JSONObject val = new JSONObject(value.toString());
					JSONObject action = val.getJSONObject("action");

					if (action.has("gameStatus")) {
						didEnd = true;
						endStatus = action.getString("gameStatus");
						score = action.getInt("points");

						String userStr = val.getString("user").substring(1);
						userId = Integer.valueOf(userStr).intValue();
					}

					nMoves++;
				}

				if (didEnd) {
					JSONObject tempObj = new JSONObject();
					tempObj.put("gameStatus", endStatus);
					tempObj.put("score", score);
					tempObj.put("moves", nMoves);

					context.write(new IntWritable(userId),
							new Text(tempObj.toString()));
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "json job");

		job.setJarByClass(activityOne.class);

		job.setMapperClass(activityOneMapper.class);
		job.setReducerClass(activityOneReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/payuen/lab7/output/prog3temp"));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}

class activityTwo extends Configured implements Tool {
	public static class activityTwoMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String valStr[] = value.toString().split("\t");
			int userId = Integer.valueOf(valStr[0]).intValue();
			context.write(new IntWritable(userId), new Text(valStr[1]));
		}
	}

	public static class activityTwoReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int won = 0;
			int lost = 0;
			int nGames = 0;
			int highScore = Integer.MIN_VALUE;
			int longestGame = Integer.MIN_VALUE;
			
			for (Text value : values) {
				try {
					nGames++;
					JSONObject val = new JSONObject(value.toString());
					String gameStatus = val.getString("gameStatus");
					
					if (gameStatus.equals("Win")) won++;
					else                          lost++;
					
					int score = val.getInt("score");
					if (score > highScore) highScore = score;
					
					int moves = val.getInt("moves");
					if (moves > longestGame) longestGame = moves;
					
				} catch(JSONException e) {
					e.printStackTrace();
				}
			}
			
			try {
				JSONObject obj = new JSONObject();
				obj.put("game", nGames);
				obj.put("won", won);
				obj.put("lost", lost);
				obj.put("highscore", highScore);
				obj.put("longestGame", longestGame);
				
				context.write(key, new Text(obj.toString()));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "json job");

		job.setJarByClass(activityTwo.class);

		job.setMapperClass(activityTwoMapper.class);
		job.setReducerClass(activityTwoReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/payuen/lab7/output/prog3temp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
