import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixCellMR {
	
	public static class TransMatrixMapper 
			extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				// invalid format or edge case
				return;
			}
			String outputKey = tuple[0];
			String [] outputVals = tuple[1].split(",");
			String len = Integer.toString(outputVals.length);
			for (String val : outputVals) {
				context.write(new Text(outputKey), new Text(val + "=1/" + len));
			}
		}
	}
	
	public static class PRateMatrixMapper
			extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				// invalid format or edge case
				return;
			}
			context.write(new Text(tuple[0]), new Text(tuple[1]));
		}
		
	}
	
	public static class MatrixCellReducer
			extends Reducer<Text, Text, Text, Text> {
		
		private double tpRate;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			tpRate = conf.getDouble("tpRate", 0);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String rate = new String();
			List<String> trans = new ArrayList<String>();
			for (Text value : values) {
				if (value.toString().contains("=")) {
					trans.add(value.toString());
				} else {
					rate = value.toString();
				}
			}
			double base = Double.parseDouble(rate);
			// write out teleport cell
			context.write(key, new Text(Double.toString(base * tpRate)));
			// write out normal transitions
			for (String tran : trans) {
				String outputKey = tran.split("=")[0];
				String [] faction = tran.split("=")[1].split("/");
				double factor = Double.parseDouble(faction[0]) / Double.parseDouble(faction[1]);
				context.write(new Text(outputKey), new Text(Double.toString(base * factor * (1.0 - tpRate))));
			}
		}
	}
	
	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("tpRate", args[3]);
		
		Job job = Job.getInstance(conf, "MatrixCellMR");
		job.setJarByClass(MatrixCellMR.class);
		
		job.setReducerClass(MatrixCellReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRateMatrixMapper.class);
		
		Path outputPath = new Path(args[2]);
		
		FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
        if (hdfsBuild.exists(outputPath))
        	hdfsBuild.delete(outputPath, true);
        
        TextOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
