#!/home/shikhar/anaconda3/bin/Rscript

library(twitteR)
#install.packages("twitteR")
library(rtweet)
#library(dplyr)
#Connect to Twitter API
consumerApi <-"ivwLJfnUpBQhYZcIeU1UhV6CB"
consumerSecret <-"UGl5WTafig4caTk3J53UxYs1l5KpavaUWTeKQQkHzc4drTblhV"
accessToken <- "1038248902602973184-aKTIntD8HJWYaf44E2CJP8tT0t31f1"
accessSecret <- "5ZjjvDwSqqlNl3d2TONbm0K3562LPqygcr53yG946WpDT"
setup_twitter_oauth(consumerApi, consumerSecret, accessToken, accessSecret)

#Request tweets with keyword. number and date
tw = twitteR::searchTwitter('#immigration', n =4000 , since = '2019-01-01')

#Remove retweets
tw = strip_retweets(tw)

#Convert to dataframe
df = twitteR::twListToDF(tw)
#Write full data to df
write.table(df,"/home/shikhar/Desktop/CS Books/SecondSem/Data Intensive Comp/lab 2/Data Collection P1/TwitterCSV/immigration.csv", sep = ",", col.names = F, append = T)
