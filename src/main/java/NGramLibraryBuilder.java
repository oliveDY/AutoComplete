import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {

	// first two <LongWritable, Text is the input key value pair
	// second two LongWritable, Text> is the output key value pair
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;

		// get noGram value
		@Override
		public void setup(Context context) {
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// inputKey: offset of the sentence in the passage
			// inputValue: change default split by line -> to split by sentence, using configuration at Driver.java

			// read sentence and do some pre-operation to sentence
			String sentence = value.toString().trim().toLowerCase().replaceAll("[^a-z]", "");

			// split into 2-gram to n-gram
			String[] words = sentence.split("\\s+");
			// we don't need 1-gram so:
			if(words.length == 1) {
				return;
			}

			StringBuilder pharse;
			for(int i = 0; i < words.length; i++) {
				pharse = new StringBuilder();
				pharse.append(words[i]);
				for(int j = 1; i + j < words.length && j < noGram; j++) {
					pharse.append(" ");
					pharse.append(words[i + j]);
					// outputKey = gram, outputValue = 1
					context.write(new Text(pharse.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// key: n-gram
			// value: 1
			//sum up the total count for each n-gram
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));

		}
	}

}