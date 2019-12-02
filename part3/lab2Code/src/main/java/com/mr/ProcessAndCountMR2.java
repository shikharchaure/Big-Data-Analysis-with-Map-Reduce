package com.mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
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
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.tartarus.snowball.ext.PorterStemmer;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;


public class ProcessAndCountMR2 {
	public static List<String> keywords = new ArrayList<String>();
	public static Set<String> stopWordList = new HashSet<String>();
	public static PorterStemmer stemmer = new PorterStemmer();
	public static	org.apache.hadoop.fs.Path stopWordFilesRead;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("setup ");
			
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
			try {
				String fn = value.toString();
				String url = "https://commoncrawl.s3.amazonaws.com/"+fn;
				URL obj = new URL(url);
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				// optional default is GET
				con.setRequestMethod("GET");
				File tempFile = File.createTempFile("prefix", ".gz");
				//tempFile.deleteOnExit();
				FileOutputStream out = new FileOutputStream(tempFile);
				IOUtils.copy(con.getInputStream(), out);
				GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(tempFile));
				InputStreamReader inStream = new InputStreamReader(gzis);	   
				BufferedReader br = new  BufferedReader(inStream);
				String line;
				boolean exists = false;
				boolean isEng = false;
				while ((line = br.readLine()) != null) {
					exists = false;
					for(String k:keywords) {
						if(line.contains(k)) {
							exists = true;
							break;
						}
					}
					if(exists) {	
						StringTokenizer tokenizer = new  StringTokenizer(line);
						while(tokenizer.hasMoreTokens()) {
							isEng = false;
							String l = tokenizer.nextToken();
							PorterStemmer stemmer = new PorterStemmer();
							CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder();
							CharsetEncoder isoEncoder = Charset.forName("ISO-8859-1").newEncoder();
							isEng = asciiEncoder.canEncode(l) || isoEncoder.canEncode(l);
							if(isEng == false)
								break;
							else {
								//EMIT
								if(stopWordList.contains(l.toLowerCase())==false) {
									l = l.replaceAll("\\p{Punct}","");
									stemmer.setCurrent(l); //set string you need to stem
									stemmer.stem();  //stem the word
									value.set(stemmer.getCurrent());
									context.write(value,new IntWritable(1));
								}

							}
						}

					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
		Job job = Job.getInstance(conf, "ProcessAndCountMR2");
		job.setJarByClass(ProcessAndCountMR2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		org.apache.hadoop.fs.Path outpath = new org.apache.hadoop.fs.Path(args[2]);	
		System.out.println("HERE");
		org.apache.hadoop.fs.Path stopWordFiles = new org.apache.hadoop.fs.Path(args[0]);
		keywords.add("Trump");
		keywords.add("President Trump");
		keywords.add("Donald Trump");
		stopWordFilesRead = new org.apache.hadoop.fs.Path(args[0]);
		System.out.println("stp "+stopWordFilesRead);
		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[1]));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[2]));
		job.waitForCompletion(true);

	}
}
