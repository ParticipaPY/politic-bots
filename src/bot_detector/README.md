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
| 1 | Account creation date [1]                             | Accounts created within the year of the election                                                                                                                                      | heuristics/simple.py        | [Creation Date](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Creation-date)        |
| 2 | Default profile [3]                                   | Accounts that contain default theme color                                                                                    | heuristics/simple.py        | [Default Account](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Default-Account)    |
| 3 | Default profile picture [3]                           | Accounts that contain default profile picture | heuristics/simple.py        | [Default Account](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Default-Account)    |
| 4 | Default background picture [3]                        | Accounts that contain default background image                                                                                     | heuristics/simple.py        | [Default Account](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Default-Account)    |
| 5 | Empty biography [3]                                   | Accounts that contain none description | heuristics/simple.py        | [Default Account](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Default-Account)    |
| 6 | Retweet bot during electoral period [2]               | Accounts that mostly do retweets during the electoral period                                                                                                                          | heuristics/simple.py        | [Retweet Bot](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Retweet-Bot)            |
| 7 | Retweet bot in general [2]                            | Accounts that mostly do retweets                                                                                                                                                      | heuristics/simple.py        | [Retweet Bot](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Retweet-Bot)            |
| 8 | Reply bot during electoral period [2]                 | Accounts that mostly do replies during the electoral period                                                                                                                          | heuristics/simple.py         |             |
| 9 | Reply bot in general [2]                              | Accounts that mostly do replies during the electoral period                                                                                                                          | heuristics/simple.py         | 
| 10| Random numbers in screen name [2]                     | Accounts that contain random numbers their screen name                                                                                                                                | heuristics/fake_handlers.py | [Fake Handler](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Handler)          |
| 11  Random letters in screen name [2]                     | Accounts that contain random letters their screen name                                                                                                                                | heuristics/fake_handlers.py | [Fake Handler](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Handler)          |
| 12| Tell-tale account names [2]                           | Account's screen names that are small variations of verified accounts                                                                                                                 | heuristics/fake_handlers.py | [Fake Handler](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Handler)          |
| 13| Absence of geographical metadata [3]                  | Accounts that do not contain geographical information                                                                                                                                 | heuristics/simple.py        | [Location](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Location)                  |
| 14| Ratio between followers and followees [3]             | Ratio between followers and followees                                                                                                               | heuristics/simple.py        | [Ratio FF](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Ratio-Followers-Followees) |
| 15| Promotion/Coordination of other bot-like accounts [2] | Accounts that primary interact with other bots                                                                                                                                        | heuristics/fake_promoter.py | [Fake Promoter](https://github.com/ParticipaPY/politic-bots/wiki/Heuristic:-Fake-Promoter)        |
| 16| Account still active                                  | Accounts that are still active                                                                                                                                        |  |         |

#### ToDo Features

| #  | Feature                                       | Description                                                                                                                                                                  |
|----|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | Tweet frequency [1]                           | Check the frequency in which the user tweets and the number of tweets per second                                                                                             |
| 2  | Sleepless account [2]                         | The user's timeline is extracted for several days and it is verified if there is no "rest" point (i.e., times in which a person should be sleeping/working and not tweeting) |
| 3  | Followers of political parody account [1]     | Check if the user follows political parody accounts                                                                                                                          |
| 4  | Activity gaps or filler content [2]           | Check if there are significant gaps in the account activity between campaigns                                                                                                |
| 5  | Metadata similarities [2]                     | Check if there are accounts that share similar creation time, profile pics, or hashtags                                                                                      |
| 6  | Tweeting out fake news or misinformation [1]  | Check if the account publishes mainly fake news or misinformation                                                                                                            |
| 7  | Stolen content [2]                            | Check if the account publishes stolen content                                                                                                                                |
| 8  | Stolen profile images [2]                     | Check if the account has as profile picture a stolen image                                                                                                                   |
| 9  | Semantic similarity [2]                       | Check if the account publishes tweets that are semantically similar to the tweets of other accounts                                                                          |

#### References

[[1](https://medium.com/@robhat/an-analysis-of-propaganda-bots-on-twitter-7b7ec57256ae)] 
Ash Bhat, Rohan Phadte. **An Analysis of Propaganda Bots on Twitter**, October 2017, Medium.

[[2](https://medium.com/data-for-democracy/spot-a-bot-identifying-automation-and-disinformation-on-social-media-2966ad93a203)]
Kris Shaffer. **Spot a Bot: Identifying Automation and Disinformation on Social Media**, June 2017, Medium.

[[3](http://journals.uic.edu/ojs/index.php/fm/article/view/7090/5653)] 
Alessandro Bessi, Emilio Ferrara. **Social bots distort the 2016 U.S. Presidential election online discussion**, 
November 2016, First Monday.

#### Other references

* [Computational Propaganda Project (Oxford)](http://comprop.oii.ox.ac.uk/)
* [Resource for understanding Political Bots](http://comprop.oii.ox.ac.uk/publishing/public-scholarship/resource-for-understanding-political-bots/)

### Run

By default, the solution takes the list of all of the authors of the tweets collected during the primary and general 
elections in Paraguay and checks their probability of being a bot. To execute, active the environment (from the root
directory `python env/bin/activate`) and then run `python bot_detector/run.py --users=[]` from the `src` directory.

#### List of users to examine
A list of Twitter handlers (without @) can be passed as part of the command line if the analysis wants to be conducted
on a subset list of tweet authors `python bot_dectector/run.py --users=["MaritoAbdo", "EfrainAlegre"]`