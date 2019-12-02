package com.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.tartarus.snowball.ext.PorterStemmer;

import com.vdurmont.emoji.EmojiParser;


public class WordCount {
	public static Set<String> stopWordList = new HashSet<String>();
	public static PorterStemmer stemmer = new PorterStemmer();
	public static	org.apache.hadoop.fs.Path stopWordFilesRead;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("setup");
			Path stopWordFiles = stopWordFilesRead;
			if (stopWordFiles != null) {
					BufferedReader br=new BufferedReader(new FileReader(stopWordFiles.toString()));
					String line = "";
					while((line = br.readLine())!=null ) {
						stopWordList.add(line);
				}
			}
		}
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
	        line = EmojiParser.removeAllEmojis(line);
			String token ;
			StringTokenizer tokenizer = new StringTokenizer(line);
			int i=0;
			while(tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				token = token.replaceAll("\\d","");
				token=token.replaceAll("[^\\p{ASCII}]", "");
				token = token.replaceAll("\\p{Punct}","");
				token = token.replaceAll("“","");
				token = token.replaceAll("’"," ");
				if(stopWordList.contains(token.toLowerCase())==false) {
					stemmer.setCurrent(token.trim()); //set string you need to stem
					stemmer.stem();  //stem the word
					token = stemmer.getCurrent();
					token = token.replaceAll("\\d"," ");
					token=token.replaceAll("[^\\p{ASCII}]", "");
					token = token.replaceAll("\\p{Punct}","");
					value.set(token.toLowerCase().trim());
					i = i+1;
					context.write(value,new IntWritable(1));
				}
			}
		}
	}


	public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable x : values) {
				sum = sum + x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		stopWordFilesRead = new org.apache.hadoop.fs.Path(args[0]);
		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[1]));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[2]));
		job.waitForCompletion(true);

	}
}
