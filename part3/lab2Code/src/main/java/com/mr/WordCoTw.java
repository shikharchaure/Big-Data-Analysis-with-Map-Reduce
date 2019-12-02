package com.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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


public class WordCoTw {
	public static Set<String> stopWordList = new HashSet<String>();
	public static Set<String> topTenWords = new HashSet<String>();
	public static PorterStemmer stemmer = new PorterStemmer();
	public static	org.apache.hadoop.fs.Path stopWordFilesRead;
	public static	org.apache.hadoop.fs.Path topTenFileRead;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public static String para;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//top10words.add("here");
			Path stopWordFiles = stopWordFilesRead;
			Path topTenFile = topTenFileRead;
			para="";
			if (stopWordFiles != null) {
				BufferedReader br=new BufferedReader(new FileReader(stopWordFiles.toString()));
				String line = "";
				while((line = br.readLine())!=null ) {
					stopWordList.add(line);
				}
			}
			
			
			topTenWords.add("trump");
			topTenWords.add("donaldtrump");
			topTenWords.add("realdonaldtrump");
			topTenWords.add("potus");
			topTenWords.add("maga");
			topTenWords.add("trumpprotest");
			topTenWords.add("new");
			topTenWords.add("presid");
			topTenWords.add("donald");
			
		}
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
			boolean exists = false;
			System.out.println("line "+line);
			String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
	        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
	        Matcher m = p.matcher(line);
	        int j = 0;
	        System.out.println("MAP ");
	        while (m.find()) {
	        	line = line.replaceAll(m.group(j),"").trim();
	            j++;
	        }
	        line = EmojiParser.removeAllEmojis(line);
			for(String top10:topTenWords) {
				if(line.contains(top10)) {
					exists= true;
				}
			}
			if(exists) {
				String[] words = line.split(" ");
				for (int i=0;i<words.length-1;i++) {
					words[i]=words[i].replaceAll("[^\\p{ASCII}]", "");
					words[i] = words[i].replaceAll("\\p{Punct}","");
					words[i] = words[i].replaceAll("\\d","");
					if(stopWordList.contains(words[i].toLowerCase())==false) {
						for(String top10:topTenWords) {
								if(words[i].toLowerCase().equals(top10)) {
									for (int k2 = 0; k2 < words.length; k2++) {
										words[k2] = words[k2].replaceAll("\\d","");
										words[k2]=words[k2].replaceAll("[^\\p{ASCII}]", "");
										words[k2] = words[k2].replaceAll("\\p{Punct}","");
										if(stopWordList.contains(words[k2].toLowerCase())==false && words[k2].trim().length()!=0) {
											value.set(words[i].toLowerCase()+","+words[k2].toLowerCase());
											System.out.println(value.toString());
											context.write(value, new IntWritable(1));
										}
										
									}

								}
						}
					}
				}
				exists = false;
			}
		}
	}


	public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			System.out.println("red ");
			int sum = 0;
			for(IntWritable x : values) {
				sum = sum + x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCoTw");
		job.setJarByClass(WordCoTw.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		org.apache.hadoop.fs.Path outpath = new org.apache.hadoop.fs.Path(args[2]);	
		stopWordFilesRead = new org.apache.hadoop.fs.Path(args[0]);
		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[1]));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[2]));
		job.waitForCompletion(true);
	}
}
