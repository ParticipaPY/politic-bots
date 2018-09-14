## Bot Detector

As part of the toolbox developed in Politic-bots, we have implemented a tool that identifies Twitter bot accounts based 
on a series of rules or heuristics. Given a user handle, the tool returns the probability of the user of being a bot. A 
set of rules represents the core of the solution. The rules were derived from various articles that describe the
features of Twitter accounts that are used to share political propaganda or to influence in the public opinion
during electoral periods. 

### Features of Political Bots

#### Implemented Features

| # | Feature                                               | Description                                                                                                                                                                           | Source Code                 | Documentation                                                                                     |
|---|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|---------------------------------------------------------------------------------------------------|
| 1 | Account Creation Date [1]                             | Accounts created within the year of the election                                                                                                                                      | heuristics/simple.py        | [Creation Date](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Creation-date)         |
| 2 | Default Twitter Account [3]                           | Accounts that contain none description and profile picture also default background image and color                                                                                    | heuristics/simple.py        | [Default Account](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Default-Account)    |
| 3 | Retweet Bot [2]                                       | Accounts that only do retweets                                                                                                                                                        | heuristics/simple.py        | [Retweet Bot](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Retweet-Bot)            |
| 4 | Tell-tale account names or name+random_num [2]        | Accounts that contain random numbers or letters as part of their name or screen name. Also account's names or screen names that are small variations of a celebrity's name or handler | heuristics/fake_handlers.py | [Fake Handler](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Handler)          |
| 5 | Absence of geographical metadata [3]                  | Accounts that do not contain geographical information                                                                                                                                 | heuristics/simple.py        | [Location](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Location)                  |
| 6 | Less followers and more followees [3]                 | Accounts whose ratio between followers and followees is greater than a given threshold                                                                                                              | heuristics/simple.py        | [Ratio FF](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Ratio-Followers-Followees) |
| 7 | Promotion/Coordination of other bot-like accounts [2] | Accounts that primary interact with other bots                                                                                                                                        | heuristics/fake_promoter.py | [Fake Promoter](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Promoter)        |

#### ToDo Features

| #  | Feature                                       | Description                                                                                                                                                                  |
|----|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | Tweet frequency [1]                           | Check the frequency in which the user tweets and the number of tweets per second                                                                                             |
| 2  | Sleepless account [2]                         | The user's timeline is extracted for several days and it is verified if there is no "rest" point (i.e., times in which a person should be sleeping/working and not tweeting) |
| 3  | Reply bot [2]                                 | Check if the vast majority of the user's activity is about replying tweets                                                                                                   |
| 4  | Followers of political parody account [1]     | Check if the user follows political parody accounts                                                                                                                          |
| 5  | Activity gaps or filler content [2]           | Check if there are significant gaps in the account activity between campaigns                                                                                                |
| 6  | Metadata similarities [2]                     | Check if there are accounts that share similar creation time, profile pics, or hashtags                                                                                      |
| 7  | Tweeting out fake news or misinformation [1]  | Check if the account publishes mainly fake news or misinformation                                                                                                           |
| 8  | Stolen content [2]                            | Check if the account publishes stolen content                                                                                                                                |
| 9  | Stolen profile images [2]                     | Check if the account has as profile picture a stolen image                                                                                                                   |
| 10 | Semantic similarity [2]                       | Check if the account publishes tweets that are semantically similar to the tweets of other accounts                                                                          |

#### References

[[1](https://medium.com/@robhat/an-analysis-of-propaganda-bots-on-twitter-7b7ec57256ae)] 
Ash Bhat, Rohan Phadte. **An Analysis of Propaganda Bots on Twitter**, October 2017, Medium.

[[2](https://medium.com/data-for-democracy/spot-a-bot-identifying-automation-and-disinformation-on-social-media-2966ad93a203)]
Kris Shaffer. **Spot a Bot: Identifying Automation and Disinformation on Social Media**, June 2017, Medium.

[[3](http://journals.uic.edu/ojs/index.php/fm/article/view/7090/5653)] 
Alessandro Bessi, Emilio Ferrara. **Social bots distort the 2016 U.S. Presidential election online discussion**, 
November 2016, First Monday.  

### Run

Currently, the solution takes all of the authors of the tweets collected during the primary and general elections in 
Paraguay and checks their probability of being a bot. To run execute `python run.py` from the directory `bot_detector` 
