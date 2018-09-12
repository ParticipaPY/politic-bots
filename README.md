# Politic Bots

Politic Bots is a side-project that started within the research project [ParticipaPY](http://participa.org.py/), which 
aims at designing and implementing solutions to generate spaces of civic participation through technology. 

Motivated by the series of journalist investigations (e.g., 
[How a Russian 'troll soldier' stirred anger after the Westminster attack](https://www.theguardian.com/uk-news/2017/nov/14/how-a-russian-troll-soldier-stirred-anger-after-the-westminster-attack), 
[Anti-Vaxxers Are Using Twitter to Manipulate a Vaccine Bill](https://www.wired.com/2015/06/antivaxxers-influencing-legislation/),
[A Russian Facebook page organized a protest in Texas. A different Russian page launched the counterprotest](https://www.texastribune.org/2017/11/01/russian-facebook-page-organized-protest-texas-different-russian-page-l/),
[La oscura utilización de Facebook y Twitter como armas de manipulación política](https://elpais.com/tecnologia/2017/10/19/actualidad/1508426945_013246.html)) 
regarding the use of social media to manipulate the public opinion, specially in times of elections, we decided to 
study the use of Twitter during the presidential elections that took place in Paraguay in
December 2017 (primary) and April 2018 (general).  

To understand how the public and the political candidates use Twitter, we collected tweets published through 
the **`accounts`** of the candidates or containing **`hashtags`** used in the campaigns. These information were 
recorded in a CSV file that was provided to the tweet collector. The source code of the collector is available at 
[here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/twitter_api_manager.py) and the CSV 
file used to pull data from Twitter during the primaries can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/internas_2017.csv).

### Data Augmentation

The accounts and hashtags employed to collect tweets during the primary elections were augmented with information about 
the parties and internal movements of the candidates. A similar approach was followed to collect tweets for the general election. However, in this case, the hashtags and 
accounts were supplemented with information not only of the candidate parties but also about the region of the 
candidates, the name of their coalitions (if any), and the political positions that they stand for. The CSV file used 
to collected tweets during the general elections can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/generales.csv). 


The functions `create_flags` and `add_values_to_flags` that annotate the tweets with information of the 
candidate's party and movement are implemented in the module `add_flags.py` in `src/tweet_collector`.

### Data Cleaning

Some of the hashtags used by the candidates were generic spanish words employed in other contexts and spanish-speaking
countries (e.g., a marketing campaign in Argentina) so, before starting any analysis, we had to ensure that the collected
tweets were actually related to the elections in Paraguay. We labeled the collected tweets as relevant if they mention 
candidate accounts or if they had at least more than one of the hashtags of interest. The class `TweetEvaluator` in
`src/utils/data_wrangler.py` contains the code that labels the collected tweets as relevant or not for this project.

## Structure of the repository

```
├── LICENSE
├── README.md                           <- The top-level README for this project.
├── requirements.txt                    <- The requirements file for reproducing the project environment, e.g.
│                                          generated with `pip freeze > requirements.txt`
├── data
│   └── raw                             <- The original, immutable data dump
├── src                                 <- Source code of the project
│   ├── __init__.py                     <- Makes src a Python module
│   ├── config.json.example             <- Example of a configuration file
│   ├── analyzer                        
│   │   └── data_analyzer.py            <- Functions to conduct analyses on tweets
│   │   └── network_analysis.py         <- Class used to conduct Social Network Analysis
│   ├── bot_detector                    
│   │   └── bot_detector.py             <- Main class to conduct the detection of bots
│   │   └── run.py                      <- Main function to execute the detection of bots
│   │   └── heuristics                  
│   │   │   └── fake_handlers.py        <- Functions to execute the heuristic fake handlers
│   │   │   └── fake_promoter.py        <- Functions to execute the heuristic fake promoter
│   │   │   └── heuristic_config.json   <- Configuration file with the parameters of the heuristics
│   │   │   └── simple.py               <- Functions to execute a set of straighforward heuristics
│   ├── tweet_collector                 
│   │   └── add_flags.py                <- Functions used to augment tweets with information about the candidates
│   │   └── generales.csv               <- CSV file with the hashtags and accounts used to collect tweets related to 
│   │   │                                  the general elections
│   │   └── internas_2017.csv           <- CSV file with the hashtags and accounts used to collect tweets related to
│   │   │                                  the primary elections
│   │   └── run.py                      <- Main function to run the tweet collector
│   │   └── tweet_collector.py          <- Class implemented to collect tweets by hitting the API of Twitter
│   ├── utils
│   │   └── data_wrangler.py            <- Functions and classes to clean and pre-process the data  
│   │   └── db_manager.py               <- Main class to operate the MongoDB used to store the tweets
│   │   └── utils.py                    <- General utilitarian functions                 
```

## Collecting Political Tweets

The data sets of tweets collected during the presidential primary and general elections that took place
in Paraguay in December 2017 and April 2018, respectively, are available in **`\data`**. They are dumps of the MongoDB
databases used in the project and can be used for free for research purposes. 

In case, a new set of tweets needs to be downloaded below we list the steps that are required to follow.

1. Go through the instructions presented [here](https://developer.twitter.com/en/docs/basics/developer-portal/guides/apps) 
and create a Twitter APP to collect tweets;  
2. Define the accounts and hashtags of interest and save them in a CSV file in a column called **keyword**. 
Additional columns can be added to the CSV to complement the information about accounts and hashtags. 
An example of CSV file can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/internas2017.csv);
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

