This project aims implementing big data analytics particularly calculating word count and word co-
occurence and visualising it over data from three sources, Twitter, New york times and common
crawl. We are needed to select a topic and collect data sub-topics for its subtopics. The
implementation of this project is divided into three parts. Data Collection, analysing data with
hadoop map reduce and visualising it using Tableau.
This data collection part is done with R and python while map reduce is done in Java.

1. Data Collection :
	Data is collected for the topic Politics.
	5 subtopics used are :
	- Trump
	- Clinton
	- Health Care
	- Obama
	- Immigration
	1) Twitter : We have used R’s twitterR library to collect data for all of the above subtopics.
	2) New York Times : We have used python’s Beautiful soap to collect data from New York
	Times API.
	We are storing these files as text files to give it as input to map-reduce job . Also storing it in
	csv for tracking.
	3) Common Crawl : Using java to hit S3 url from common crawl and downloading only the
	topics required instead of downloading big data.

2. Big Data Analysis

	There are two operations peformed in this stage :
	2.1. Word Count
	2.2. Word Co-occurence
	We have used Amazon S3 to run map reduce jobs.
	Before applying these operations we need to do text processing which includes removal of stop
	words, links and other non-ascii characters.
