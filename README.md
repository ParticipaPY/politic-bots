# Politic Bots

Politic Bots is a side-project that started within the research project [ParticipaPY](http://participa.org.py/), which 
aims at designing and implementing solutions to generate spaces of civic participation through technology. 

Motivated by the series of journalist investigations (e.g., 
[How a Russian 'troll soldier' stirred anger after the Westminster attack](https://www.theguardian.com/uk-news/2017/nov/14/how-a-russian-troll-soldier-stirred-anger-after-the-westminster-attack), 
[Anti-Vaxxers Are Using Twitter to Manipulate a Vaccine Bill](https://www.wired.com/2015/06/antivaxxers-influencing-legislation/),
[A Russian Facebook page organized a protest in Texas. A different Russian page launched the counterprotest](https://www.texastribune.org/2017/11/01/russian-facebook-page-organized-protest-texas-different-russian-page-l/),
[La oscura utilización de Facebook y Twitter como armas de manipulación política](https://elpais.com/tecnologia/2017/10/19/actualidad/1508426945_013246.html)) 
regarding the use of social media to manipulate the public opinion, specially in times of elections, we decided to 
study the use of Twitter during the electoral period of the Paraguayan 2017 primary elections.  

To understand how the public and the political candidates use Twitter, we collected tweets published through 
the **accounts** of the candidates or containing **hashtags** used in the campaigns. The accounts
and hashtags employed to collect tweets during the primary elections were augmented with information about the
parties and internal movements of the candidates. All of these information were recorded in a CSV file that was 
provided to the tweet collector. The source code of the collector is available at `src/tweet_collector/twitter_api_manager.py` and
the CSV file used to pull data from Twitter during the primaries can be found at `src/tweet_collector/kwd_metadata.csv`.

## Collecting Political Tweets

1. Follow the instructions presented [here](https://developer.twitter.com/en/docs/basics/developer-portal/guides/apps) 
to create a Twitter APP that will be used to collect tweets;  
2. Define the accounts and hashtags of interest and save them in a CSV file in a column called **keyword**. 
Additional columns can be added to the CSV to complement the information about accounts and hashtags. 
An example of CSV file can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/kwd_metadata.csv);
3. Rename `src/config.json.example` to `src/config.json`;
4. Set to **metadata** in `src/config.json` the path to the CSV file; 
5. Set to **consumer_key** and **consumer_secret** in `src/config.json` the information of the authentication tokens 
of the Twitter App created in the step 1;
6. Set in `src/config.json` the information of the MongoDB database that will be used to store the tweets;
7. Execute `python src/tweet_collector/run.py`

Depending on the number of hashtags and accounts the collection can take several hours.

## Bot Detector

As part of the toolbox it is included an algorithm that identify Twitter bot accounts based on a series of heuristics.

Algorithm to detect Twitter bots. Given a user handle, it returns the probability of the user of being a bot. The algorithm is based on 18 heuristics, which are described below.

