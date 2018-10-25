# Politic Bots

Politic Bots is a side-project that started within the research project [ParticipaPY](http://participa.org.py/) of the
Catholic University "Nuestra Señora de la Asunción", which aims at designing and implementing solutions to generate 
spaces of civic participation through technology. 

Motivated by the series of journalist investigations (e.g., 
[How a Russian 'troll soldier' stirred anger after the Westminster attack](https://www.theguardian.com/uk-news/2017/nov/14/how-a-russian-troll-soldier-stirred-anger-after-the-westminster-attack), 
[Anti-Vaxxers Are Using Twitter to Manipulate a Vaccine Bill](https://www.wired.com/2015/06/antivaxxers-influencing-legislation/),
[A Russian Facebook page organized a protest in Texas. A different Russian page launched the counterprotest](https://www.texastribune.org/2017/11/01/russian-facebook-page-organized-protest-texas-different-russian-page-l/),
[La oscura utilización de Facebook y Twitter como armas de manipulación política](https://elpais.com/tecnologia/2017/10/19/actualidad/1508426945_013246.html),
[How Bots Ruined Clicktivism](https://www.wired.com/story/how-bots-ruined-clicktivism/)) 
regarding the use of social media to manipulate the public opinion, specially in times of elections, we decided to 
study the use of Twitter during the presidential elections that took place in Paraguay in
December 2017 (primary) and April 2018 (general).  

To understand how the public and the political candidates use Twitter, we collected tweets published through 
the **`accounts`** of the candidates or containing **`hashtags`** used in the campaigns. This information was 
recorded in a CSV file that was provided to the tweet collector. The source code of the collector is available at 
[here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/twitter_api_manager.py) and the CSV 
file used to pull data from Twitter during the primaries can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/internas_2017.csv).

### Data Augmentation

The accounts and hashtags employed to collect tweets during the primary elections were augmented with information about 
the parties and internal movements of the candidates. A similar approach was followed to collect tweets for the general election. However, in this case, the hashtags and 
accounts were supplemented with information not only of the candidate parties but also about the region of the 
candidates, the name of their coalitions (if any), and the political positions that they stand for. The CSV file used 
to collect tweets during the general elections can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/generales.csv). 


The functions `create_flags` and `add_values_to_flags` that annotate the tweets with information of the 
candidate's party and movement are implemented in the module `add_flags.py` in `src/tweet_collector`.

### Data Cleaning

Some of the hashtags used by the candidates were generic Spanish words employed in other contexts and Spanish-speaking
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
├── sna                                 <- Social Network Analysis
│   ├── gefx                            <- Files that record the interaction network among users
│   ├── img                             <- Images that illustrate the interaction network among users
├── reports                             <- Reports about the usage of Twitter during elections in Paraguay
│   ├── notebooks                       <- Jupyter notebooks used to conduct the analyses
│   ├── html                            <- HTML files with the results of the analyses
├── src                                 <- Source code of the project
│   ├── __init__.py                     <- Makes src a Python module
│   ├── run.py                          <- Main script to run analysis tasks
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
│   │   └── tweet_collector.py          <- Class implemented to collect tweets by hitting the API of Twitter
│   ├── utils
│   │   └── data_wrangler.py            <- Functions and classes to clean and pre-process the data  
│   │   └── db_manager.py               <- Main class to operate the MongoDB used to store the tweets
│   │   └── utils.py                    <- General utilitarian functions                 
```

## Analyses

The directory `reports` contains the analyses conducted to study the use of Twitter during the primary and general
elections. [Jupyter notebook](http://jupyter.org/) was employed to document the analyses and report the results. HTML
files were generated to facilitate the access to the analyses and results.

## Getting Started

### Installation guide

Links to packages are provided below.

1. Download and install Python >= 3.4.4;
2. Download and install MongoDB community version;
3. Create a Twitter APP by following the instructions [here](https://developer.twitter.com/en/docs/basics/developer-portal/guides/apps);
4. Clone the repository `git clone https://github.com/ParticipaPY/politic-bots.git`;
5. Get into the directory of the repository `cd politic-bots`;
6. Create a virtual environment by running `virtualenv env`;
7. Activate the virtual environment by executing `source env/bin/activate`;
8. Inside the directory of the repository install the project dependencies by running `pip install -r requirements.txt`;

*Optional* for social network analysis: Download and install Gephi  

## Run

There are some tasks that can be run from `src/run.py`. Bellow we explain each of them.

### Pre-requirements

1. Set in `src/config.json` the information of the MongoDB database that is used to store the tweets;
2. Activate the virtual environment by executing `source env/bin/activate`.

### Collect Political Tweets

The data sets of tweets collected during the presidential primary and general elections that took place
in Paraguay in December 2017 and April 2018, respectively, are available to be download.

In case a new set of tweets needs to be downloaded, below we list the steps that are required to follow.
 
1. Create a CSV file to contain the name of the Twitter accounts and hashtags of interest. The CSV file should have
a column called **keyword** with the list of accounts and hashtags. Additional columns can be added to the CSV to 
complement the information about accounts and hashtags. An example of CSV file can be found [here](https://github.com/ParticipaPY/politic-bots/blob/master/src/tweet_collector/internas2017.csv);
2. Rename `src/config.json.example` to `src/config.json`;
3. Set to **metadata** in `src/config.json` the path to the CSV file; 
4. Set to **consumer_key** and **consumer_secret** in `src/config.json` the information of the authentication tokens 
of the Twitter App created during the installation process;
5. Go to the `src` directory and execute `python run.py --collect_tweets`.

Depending on the number of hashtags and accounts the collection can take several hours or even days.

### Create database of tweet authors

Before conducting analyses on the tweets, a database of the authors of tweets should be created. To create the database
of users active the virtual environment `source env/bin/activate` and execute from the `src` directory 
`python run.py --db_users`. A new MongoDB database so called `users` is created as a result of this process.

### Analyze the sentiment of tweets

It is possible to analyze the tone of tweets by executing, from the `src` directory, `python run.py --sentiment_analysis`. 
The sentiment of tweets are stored as part of the dictionary that contains the information of the tweet under the key 
`sentimiento`. We use the library CCA-Core to analyze the sentiment embed in Tweets. 
See [here](https://github.com/ParticipaPY/cca-core) for more information about the CCA-Core library.

### Identify relevant tweets

Tweets should be evaluated to analyze their relevance for this project. See **Data Cleaning** section to understand
the problems with the hashtags used to collect tweets. From the `src` directory of the repository and after activating
your virtual environment `source env/bin/activate`, run `python run.py --flag_tweets` to perform both tasks. The 
flag `relevante`, added to the dictionary that stores the information of the tweets, indicates whether the tweet is
relevant or not for the purpose of this project.

### Generate network of interactions

Once the database of users was generated a network that shows the interactions among them can be created for a 
follow-up social network analysis. From the `src` directory and after activating your virtual environment 
(`source env/bin/activate`), run `python run.py --interaction_net` to generate the network of interactions 
among the tweet authors. Examples of interaction networks can be found in the directory `sna` of the repo.

### Troubleshooting

If you get the error **`ImportError: No module named`** when trying to execute the scripts, make sure to be at the
`src` directory. If after being at the `src` directory you still get the same error, it is possible that you need to add
the `src` directory to the `PYTHONPATH` by adding `PYTHONPATH=../` at the beginning of the execution commands, e.g., 
`PYTHONPATH=../ python analyzer/pre_analysis.py`   

## Technologies

1. [Python 3.4](https://www.python.org/downloads/)
2. [MongoDB](https://www.mongodb.com/download-center#community)
3. [Tweepy](https://github.com/tweepy/tweepy)
4. [Gephi](https://gephi.org/)

## Issues

Please use [Github's issue tracker](https://github.com/ParticipaPY/politic-bots/issues/new) to report issues and suggestions.

## Contributors

[Jammily Ortigoza](https://github.com/jammily), [Jorge Saldivar](https://github.com/joausaga), 
[Josué Ibarra](https://github.com/josueibarra95), [Laura Achón](https://github.com/lauraachon),
[Cristhian Parra](https://github.com/cdparra)