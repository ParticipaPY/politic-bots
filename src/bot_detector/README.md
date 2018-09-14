## Bot Detector

As part of the toolbox developed in Politic-bots, we have implemented a tool that identifies Twitter bot accounts based 
on a series of rules or heuristics. Given a user handle, the tool returns the probability of the user of being a bot. A 
set of rules represents the core of the solution. The rules were derived from various articles that describe the
features of Twitter accounts that are used to share political propaganda or to influence in the public opinion
during electoral periods.

The solution checks the probability of being bot of the authors of the tweets collected during the primary and general
elections in Paraguay.  

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

...

#### References

[[1](https://medium.com/@robhat/an-analysis-of-propaganda-bots-on-twitter-7b7ec57256ae)] 
Ash Bhat, Rohan Phadte. **An Analysis of Propaganda Bots on Twitter**, October 2017, Medium.

[[2](https://medium.com/data-for-democracy/spot-a-bot-identifying-automation-and-disinformation-on-social-media-2966ad93a203)]
Kris Shaffer. **Spot a Bot: Identifying Automation and Disinformation on Social Media**, June 2017, Medium.

[[3](http://journals.uic.edu/ojs/index.php/fm/article/view/7090/5653)] 
Alessandro Bessi, Emilio Ferrara. **Social bots distort the 2016 U.S. Presidential election online discussion**, 
November 2016, First Monday.  

### Run

 
