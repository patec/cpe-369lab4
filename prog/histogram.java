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

import java.awt.Point;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import javax.lang.model.SourceVersion;

public class histogram extends Configured implements Tool {

	public static class hMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				JSONObject obj = new JSONObject(value.toString());
				JSONObject loc = obj.getJSONObject("action")
						.getJSONObject("location");
				MyPoint pos = new MyPoint(loc.getInt("x"), loc.getInt("y"));
				context.write(new Text(pos.toString()), new IntWritable(1));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static class hReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : value) {
				sum += val.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "json job");

		job.setJarByClass(histogram.class);

		job.setMapperClass(hMapper.class);
		job.setReducerClass(hReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new histogram(), args);
		System.exit(res);
	}
}

class MyPoint extends Point {
	public MyPoint(int x, int y) {
		super(x, y);
	} 
	
	@Override
	public String toString() {
		return "[x= " + x + ", y= " + y + "]";
	}
}
