#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 22:03:36 2019

@author: shikhar
"""
import requests
import urllib
import json
from bs4 import BeautifulSoup
import urllib.request
import csv

file = open('/home/shikhar/Desktop/CS Books/SecondSem/Data Intensive Comp/lab 2/Data Collection P1/healthCare.txt','a+')
pageFile = open('/home/shikhar/Desktop/CS Books/SecondSem/Data Intensive Comp/lab 2/Data Collection P1/webUrlPage.txt','r')
pageNum = pageFile.read()
pageFile.close()
pageNum = pageNum.strip()
pageNum = int(pageNum)
if(pageNum>100):
	quit()

count = 0
for page in range(pageNum,pageNum+9):
    count = count + 1
    response=requests.get('https://api.nytimes.com/svc/search/v2/articlesearch.json?q=healthcare&page='+str(page)+'&begin_date=20190202&api-key=Q3tkn2MoAA5A9QAiBYCmv8maqh3TVV68')
    jsonRes = json.loads(response.text)
    web_urls = jsonRes['response']['docs']
    for web_url in web_urls:
        url = web_url['web_url']
        lead_par = web_url['lead_paragraph']
        pub_date = web_url['pub_date']
        with open('/home/shikhar/Desktop/CS Books/SecondSem/Data Intensive Comp/lab 2/Data Collection P1/NYT CSV/healthCare.csv',mode = 'a') as csvfile:    
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([url,lead_par,pub_date])
        html = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(html)
        
        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        
        # get text
        
        text = soup.get_text()
        
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        end = text.find("Order Reprints | Today’s Paper | SubscribeAdvertisementOpen")
        if(end!=-1):
            fileContent= text[0:end]
            file.write(fileContent+'\n'+'\n')
        text = ""
        end = ""
        
file.close()
filePageW = open('/home/shikhar/Desktop/CS Books/SecondSem/Data Intensive Comp/lab 2/Data Collection P1/webUrlPage.txt','w')
updatedPageNum = int(pageNum) + count
filePageW.write(str(updatedPageNum))
filePageW.close()















