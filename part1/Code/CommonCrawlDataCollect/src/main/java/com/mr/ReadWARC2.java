package com.mr;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.tika.language.LanguageIdentifier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

public class ReadWARC2 {
	public static Set<String> stopWordList = new HashSet<String>();
	public static void main(String[] args) throws IOException {
		FileReader frs = new FileReader("/home/shikhar/Desktop/stopWords.txt");
		BufferedReader brs=new BufferedReader(frs);
		String line = "";
		while((line = brs.readLine())!=null ) {
			stopWordList.add(line);
		}
		int articleCount = 0;
		List<String> keywords = new ArrayList<String>();
		
		boolean exists = false;
		keywords.add("healthCare");
		keywords.add("HEALTHCARE");
		keywords.add("HEALTH CARE");
		keywords.add("health care");
		keywords.add("health");
		//keywords.add("Donald Trump");
		FileWriter fw = new FileWriter("/home/shikhar/Desktop/CCDATA/obama.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		FileReader fr =new FileReader("/home/shikhar/Desktop/WET/CC-MAIN-20190215183319-20190215205319-00000.warc.wet");
		AWSCredentials credentials = new BasicAWSCredentials(
				"AKIAI3GTSNNLYE2E5G7Q", 
				"RE/sx29DkvZhbQhmiTOGI6kThv8W44iObR3IJO8j"
				);
		AmazonS3 s3 = (AmazonS3) AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).
				withRegion(Regions.US_EAST_1).build();
		String fn = "crawl-data/CC-MAIN-2019-09/segments/1550247479101.30/wet/CC-MAIN-20190215183319-20190215205319-00000.warc.wet.gz";
		S3Object f = s3.getObject("commoncrawl",fn);
		InputStream objectData = f.getObjectContent();
		GZIPInputStream gzInputStream=new GZIPInputStream(objectData);
		InputStreamReader inStream = new InputStreamReader(gzInputStream);	   
		BufferedReader br = new  BufferedReader(fr);
		String para=" ";
		byte[] b = para.getBytes();
		while((line = br.readLine())!=null) {
			int count =1;
			if(line.contains("WARC/1.0")) {
				count = count+1;
				if(count == 2) {
					//System.out.print("para "+para);
					String s = new String(b);

					LanguageIdentifier identifier = new LanguageIdentifier(s);
					if(identifier.getLanguage().equals("en")) {
						for(String k:keywords) {
							if(s.contains(k)) {
								exists = true;
								break;
							}
						}
					}
					if(exists) {	
						if(articleCount == 100)
							break;
						articleCount = articleCount+1;
						System.out.println("art "+articleCount);
						bw.write("Article  "+articleCount+"\n");
						bw.write(s+"\n");
//						StringTokenizer tokenizer = new  StringTokenizer(s);
//						while(tokenizer.hasMoreTokens()) {
//							String l = tokenizer.nextToken();
//							PorterStemmer stemmer = new PorterStemmer();
//							if(stopWordList.contains(l.toLowerCase())==false) {
//								l = l.replaceAll("\\p{Punct}","");
//								stemmer.setCurrent(l); //set string you need to stem
//								stemmer.stem();
//								String outp = stemmer.getCurrent();
//								identifier = new LanguageIdentifier(outp);
//								if(identifier.getLanguage().equals("en")) {
//									System.out.println("out is "+outp.toLowerCase()+" 1");
//								}
//							}
//
//						}
						exists = false;
					}
					para = "";
					b = null;
					count = 0;
				}
			}
			para = para + line;
			b = para.getBytes();

		}
		System.out.println("Out at art "+articleCount);
	}
}
