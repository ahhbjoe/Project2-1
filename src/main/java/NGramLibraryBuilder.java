
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//inputKey: offset
			//inputValue: line -> sentence
			//read sentence -> split into 2gram-ngram
			//write to disk
			//outputKey = gram
			//outputValue = 1

			//split sentence into words
			//i love big data n=3
			//i love, i love big
			//love big, love big data
			//big data



			String sentence = value.toString().toLowerCase().replaceAll("[^a-z]", " ").trim();
			String[] words = sentence.split("\\s+");
			if (words.length < 2) {
				return;
			}

			StringBuilder phrase;
			for (int i = 0; i < words.length; i++) {
				phrase = new StringBuilder();
				phrase.append(words[i]);
				for (int j = 1; i + j < words.length && j < noGram; j++) {
					phrase.append(" ");
					phrase.append(words[i+j]);
					context.write(new Text(phrase.toString().trim()), new IntWritable(1));
				}
			}

		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key = ngram big data
			//value = 1, 1, 1, 1....
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

}