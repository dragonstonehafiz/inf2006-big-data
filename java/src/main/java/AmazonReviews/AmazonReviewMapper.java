package AmazonReviews;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;

import Utility.Review;
import Utility.TimestampConverter;

public class AmazonReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text rating = new Text();
	IntWritable one = new IntWritable(1);
	private Gson gson = new Gson();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Get each json line
		String[] jsonLines = value.toString().split("\n");
		
		// Loop through each json
		for (int i = 0; i < jsonLines.length; ++i)
		{
			String jsonLine = jsonLines[i];
			try {
				Review review = gson.fromJson(jsonLine, Review.class);
				
				// Ignore reviews that are too short (could be spam)
				// Ignore reviews that are not from people who have bought the product
				if (review.text.length() < 10 || !review.verified_purchase)
					continue;
				
				String monthYear = TimestampConverter.convert_yy_mm(review.timestamp);
				String sentiment = review.sentiment;
			    int rating = (int) review.rating;
			    // Compose the key
			    String compositeKey = monthYear + "-" + sentiment + "-" + String.valueOf(rating) + "-" +  review.asin;
			    context.write(new Text(compositeKey), new IntWritable(1));
				
			}
			catch (Exception e) {
				System.err.println("Failed to parse line:" + jsonLine);
			}
		}
		
		
		
//		String[] parts = value.toString().split("\t");
//		String shapeStr;
//		if(parts.length == 6 && parts[3] != null) {
//			shapeStr = parts[3].trim();
//			if(shapeStr != null && !shapeStr.isEmpty()) {
//				shape.set(shapeStr);
//				context.write(shape, one);
//			}
//		}
	}
}
