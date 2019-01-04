import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			//get the threashold parameter from the configuration
			Configuration configuration = context.getConfiguration();
			threashold = configuration.getInt("threashold", 10);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// inputKey: offset of the sentence in the passage
			// input value format: KeyValue\tCountValue e.g. I love big\t100
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}

			String[] phase_count = value.toString().trim().split("\t");
			int count = Integer.parseInt(phase_count[1]);
			// filter the n-gram lower than threashold
			if(count < threashold) {
				return;
			}

			// output format: this is --> cool = 20
			String[] words = phase_count[0].split(" ");
			StringBuilder outputKey = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++) {
				outputKey.append(words[i] + " ");
			}

			String outputValue = words[words.length - 1] + "=" + count;

			//write key-value to reducer
			context.write(new Text(outputKey.toString().trim()), new Text(outputValue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// inputKey : I love big
			// inputValue : <apple = 100, house = 200, farm = 300....>

			// sort based on count get TOP K using treemap
			// data format in treemap: <300, {apple, grape, banana....}>
			TreeMap<Integer, List<String>> tm = new TreeMap(Collections.<Integer>reverseOrder());// override the default ascending order to decending order
			for(Text value : values) {
				String curValue = value.toString().trim();
				int count = Integer.parseInt(curValue.split("=")[1]);
				String word = curValue.split("=")[0];
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}

			// iterate the top K count value in treemap
			// if many word with the same count, write them all to output
			Iterator<Integer> iterator = tm.keySet().iterator();
			for(int i = 0; i < n;) {
				int count = iterator.next();
				List<String> curList = tm.get(count);
				for(String curWord: curList) {
					context.write(new DBOutputWritable(key.toString(), curWord, count), NullWritable.get());
				}
			}


		}
	}
}
