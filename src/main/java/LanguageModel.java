
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;
		// get the threshold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value: love big data\t100
			//outputKey: love big
			//outputValue: data=100

			String[] phrase_count = value.toString().trim().split("\t");
			int count = Integer.parseInt(phrase_count[1]);
			if (count < threshold) {
				return;
			}

			String[] words = phrase_count[0].split(" ");
			//love, big, data
			StringBuilder outputKey = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				outputKey.append(words[i] + " ");
			}
			//love big

			String outputValue = words[words.length - 1] + "=" + count;
			//data=100

			context.write(new Text(outputKey.toString().trim()), new Text(outputValue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			topK = context.getConfiguration().getInt("topK", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//inputKey = love big
			//inputValue = <data=100, island=28, girl=132...>
			//sort based on count -> topK
			//insert into database -> 3 columns: inputPhrase, followingWord, count

			//TreeMap<Count, List<word>>
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text value: values) {
				String curValue = value.toString().trim();
				//data=100
				String word = curValue.split("=")[0];
				int count = Integer.parseInt(curValue.split("=")[1]);
				if (tm.containsKey(count)) {
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iterator = tm.keySet().iterator();
			for (int j = 0; j < topK;) {
				if (!iterator.hasNext()) {
					break;
				}
				int count = iterator.next();
				List<String> words = tm.get(count);
				for (String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, count), NullWritable.get());
				}
			}
		}
	}
}
