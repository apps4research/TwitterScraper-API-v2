# Command line usage 
# python3 ./Tweet-Full-Archive-Raw.py Twitterdev 2019-12-23
# where Twitterdev is the Twitter account name you would like to retrieve
# and the date is the oldest tweet that the script should retrieve
# This script gets all available twitter fields

# Tina Frieda Keil, t.keil@lancaster.ac.uk, 2022
# adpated from script by AndrewEdward37 (Github), 
# see https://gist.github.com/AndrewEdward37/3fa635dc85c9027cec71e5ab0735230c

# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For retrieving of command line variables - here the twitter account name
import sys

# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata

# For URLs
import re

#To add wait time between requests
import time

#Twitter Authentication Variables
os.environ['TOKEN'] = '--- Enter your Bearer Token here ---'

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,referenced_tweets.id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'next_token': {}}
    #print(query_params)
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_full_retweet_text(rtid):
    for rt in json_response['includes']['tweets']:
        if rtid == rt['id']:
            rt_text = rt['text']
            rt_count = rt['public_metrics']['retweet_count']
            rt_reply_count = rt['public_metrics']['reply_count']
            rt_likes = rt['public_metrics']['like_count']
            rt_quote_count = rt['public_metrics']['quote_count']
            return (rt_text, rt_count, rt_reply_count, rt_likes, rt_quote_count)

def get_reply_username(reply_to_userid):
    for rtu in json_response['includes']['users']:
        if reply_to_userid == rtu['id']:
            reply_username = rtu['username']
            return (reply_username)

def append_to_csv(json_response, fileName):

    #counter variable
    counter = 0

    #print(json_response)

    #get screenname, does not need to be looped as it will always be the same
    screen_name = json_response['includes']['users'][0]['username']
   
    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    #Loop through each tweet
    for tweet in json_response['data']:
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # Author ID
        author_id = tweet['author_id']

        # Time created
        created_at = dateutil.parser.parse(tweet['created_at'])       

        # Tweet ID
        tweet_id = tweet['id']

        # Language
        lang = tweet['lang']

        # Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # Tweet text raw
        text = tweet['text']

        is_retweet = 'FALSE'
        is_quote = 'FALSE'
        reply_to_tweet_id = 'NA'
        reply_to_user_id = "NA"
        reply_to_screen_name = "NA"
        rt_id = 0
      
        try:
            tweet_type = tweet['referenced_tweets'][0]['type']
        except KeyError:
            tweet_type = ""

        # Is Retweet?
        if tweet_type == 'retweeted':
            is_retweet = 'TRUE'
            #get the full text of the retweet
            rt_id = tweet['referenced_tweets'][0]['id']
            rt_details = get_full_retweet_text(rt_id)
            text = rt_details[0]
            retweet_count = rt_details[1]
            reply_count = rt_details[2]
            like_count = rt_details[3]
            quote_count = rt_details[4]

        # Is Quote?
        if tweet_type == 'quoted':
            is_quote = 'TRUE'

        source = tweet['source']
        num_text_chars = len(text)

        # Replied to details
        if tweet_type == 'replied_to':
            reply_to_tweet_id = tweet['referenced_tweets'][0]['id']
            reply_to_user_id = tweet['in_reply_to_user_id']
            reply_to_screen_name = get_reply_username(reply_to_user_id)

        # Assemble all data in a list   
        res = [author_id, tweet_id, created_at, screen_name, text, source, num_text_chars, reply_to_tweet_id, reply_to_user_id, reply_to_screen_name, is_quote, is_retweet, reply_count, retweet_count, like_count, quote_count, lang]
        
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter) 

################################################

#Inputs for tweets
bearer_token = auth()
headers = create_headers(bearer_token)

today = datetime.date.today()
now = datetime.datetime.now() - datetime.timedelta(hours=1)
current_datetime = now.strftime("%Y-%m-%dT%H:%M:%S")+".000Z"

#retrieve and assign command line vars and
#do some basic error handling if inputs are missing
try:
    username = sys.argv[1] #twitter account name
except IndexError:
    print('** Please enter a valid Twitter Username **')
    sys.exit(1)

try:
    start_date = sys.argv[2] #start date
except IndexError:
    print('** Please enter a valid start date for tweets you want to retrieve **')
    sys.exit(1)  # abort because of error

try:
    end_date = sys.argv[3] #start date
except IndexError:
    end_date = today

keyword = "from:"+username

# start and end dates for queries which will only deliver a max of 500 tweets, 
# thus we need to limit and split. Here I am assuming the user hasn't posted 
# more than 500 tweets in 3 months

start_range = pd.date_range(start=start_date,end=end_date,freq='MS',tz='Europe/London')
end_range   = pd.date_range(start=start_date,end=end_date,freq='M',tz='Europe/London')

start_list = pd.DataFrame(index=start_range)
start_list.index = start_list.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z")
start = start_list.index.astype(str).values.tolist()

end_list = pd.DataFrame(index=end_range)
end_list.index = end_list.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d')+'T23:59:59.000Z')
end = end_list.index.astype(str).values.tolist()
end.append(str(current_datetime))

#print(start)
#print(end)

max_results = 500 #number of tweets retrievable per request

#Total number of tweets we collected from the loop
total_tweets = 0 

#data file name
datafile = username+'.csv'

# Create file
csvFile = open(datafile, "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)

#Create headers for the data you want to save, in this example, we only want save these columns in our dataset
csvWriter.writerow(['author id', 'tweet_id', 'created_at', 'author_username', 'tweet', 'source', 'num_text_chars', 'reply_to_tweet_id', 'reply_to_user_id', 'reply_to_screen_name', 'is_quote', 'is_retweet', 'reply_count', 'retweet_count', 'like_count', 'quote_count', 'lang'])
csvFile.close()

for i in range(0,len(start)):

    # Inputs
    count = 0 # Counting tweets per time period
    max_count = 100 # Max tweets per time period
    flag = True
    next_token = None
    wait_time = 2 #min 1
    
    # Check if flag is true
    while flag:
        # Check if max_count reached
        if count >= max_count:
            break
        print("\n")
        print("Request:", str(i+1))
        print("Token: ", next_token)
        url = create_url(keyword, start[i],end[i], max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']
            print("Next Token: ", next_token)
            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start[i])
                append_to_csv(json_response, datafile)
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("------------------")
                time.sleep(wait_time)              
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                print("-------------------")
                print("Start Date: ", start[i])
                append_to_csv(json_response, datafile)
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("------------------")
                time.sleep(wait_time)
            
            #Since this is the final request, turn flag to false to move to the next time period.
            flag = False
            next_token = None
        time.sleep(wait_time)
print("\nTotal number of results: ", total_tweets)