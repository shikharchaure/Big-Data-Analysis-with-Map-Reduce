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


public class WordCoMR {
	public static Set<String> stopWordList = new HashSet<String>();
	public static Set<String> topTenWords = new HashSet<String>();
	public static PorterStemmer stemmer = new PorterStemmer();
	public static PorterStemmer stemmer2 = new PorterStemmer();
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
			if (topTenFile != null) {
				BufferedReader br=new BufferedReader(new FileReader(topTenFile.toString()));
				String line = "";
				while((line = br.readLine())!=null ) {
					topTenWords.add(line);
					System.out.println(line);
				}
			}
		}
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
	        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
	        Matcher m = p.matcher(line);
	        int j = 0;
	        while (m.find()) {
	        	line = line.replaceAll(m.group(j),"").trim();
	            j++;
	        }
	        line = EmojiParser.removeAllEmojis(line);
			String token ;//="/home/shikhar/demoStopWords/input/NytTrump.txt";
			boolean exists = false;
			byte[] b = para.getBytes();
			int count =1;
			//System.out.println("line "+line.contains("- The New York Times"));
			if(line.contains("- The New York Times")) {
				count = count+1;
				if(count == 2) {
					String s = new String(b);
					for(String top10:topTenWords) {
						if(s.contains(top10)) {
							exists= true;
						}
					}
					if(exists) {
					String[] words = s.split(" ");
					for (int i=0;i<words.length-1;i++) {
						words[i]=words[i].replaceAll("[^\\p{ASCII}]", "");
						words[i] = words[i].replaceAll("\\p{Punct}","");
						words[i] = words[i].replaceAll("\\d"," ");
						if(stopWordList.contains(words[i].toLowerCase())==false) {
							for(String top10:topTenWords) {
									if(words[i].toLowerCase().equals(top10)) {
										for (int k2 = 0; k2 < words.length; k2++) {
											words[k2] = words[k2].replaceAll("\\d","");
											words[k2]=words[k2].replaceAll("[^\\p{ASCII}]", "");
											words[k2] = words[k2].replaceAll("\\p{Punct}","");
											if(stopWordList.contains(words[k2].toLowerCase())==false && words[k2].trim().length()!=0) {
												stemmer.setCurrent(words[i].toLowerCase()); //set string you need to stem
												stemmer.stem();  //stem the word
												token = stemmer.getCurrent();
												
												stemmer2.setCurrent(words[k2].toLowerCase()); //set string you need to stem
												stemmer2.stem();  //stem the word
												String token2 = stemmer2.getCurrent();
												
												value.set(token+","+token2);
												context.write(value, new IntWritable(1));
											}
											
										}
//										if(i-1>0) {
//											value.set(words[i]+","+words[i-1]);
//											context.write(value, new IntWritable(1));
//										}
//										if(i+1<words.length) {
//											value.set(words[i]+","+words[i+1]);
//											System.out.println();
//											context.write(value, new IntWritable(1));
//										}
									}
							}
						}
					}
					exists = false;
				}
					para = "";
					b = null;
					count = 0;
				}
			}
			//line=line.replaceAll("\\p{Punct}","");
			//line= line.replaceAll("\\d","");
			para = para + line;
			b = para.getBytes();
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
		Job job = Job.getInstance(conf, "WordCoMR");
		job.setJarByClass(WordCoMR.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		org.apache.hadoop.fs.Path outpath = new org.apache.hadoop.fs.Path(args[3]);	
		System.out.println("HERE");
		org.apache.hadoop.fs.Path stopWordFiles = new org.apache.hadoop.fs.Path(args[0]);
		System.out.println("HERE2");
		System.out.println("HERE26");
		stopWordFilesRead = new org.apache.hadoop.fs.Path(args[0]);
		topTenFileRead = new org.apache.hadoop.fs.Path(args[1]);
		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[2]));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[3]));
		job.waitForCompletion(true);

	}
}
