# Politic Bots

Tools and algorithms to analyze Paraguayan Tweets in times of elections. As part of the toolbox it is included an algorithm that identify Twitter bot accounts based on a series of heuristics.


## Bot Dectetor

Algorithm to detect Twitter bots. Given a user handle, it returns the probability of the user of being a bot. The algorithm is based on 18 heuristics, which are described below.

### Heuristic: Fake Handlers

Heuristic that allows identifying user names that contain a random string of numbers and letters or that are similar to user names used by public figures, for example @maritoabdo (true) --- current president of Paraguay---, @marioabdojunior (fake).