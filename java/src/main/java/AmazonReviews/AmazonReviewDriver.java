package AmazonReviews;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AmazonReviewDriver 
{
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Amazon Review Analysis");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		job.addCacheFile(new URI(otherArgs[2]));
		
		job.setJarByClass(AmazonReviewDriver.class);
		job.setMapperClass(AmazonReviewMapper.class);
		job.setCombinerClass(AmazonReviewReducer.class);
		job.setReducerClass(AmazonReviewReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
