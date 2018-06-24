# Politic Bots

Tools and algorithms to analyze Paraguayan Tweets in times of elections. As part of the toolbox it is included an algorithm that identify Twitter bot accounts based on a series of heuristics.


## Bot Detector

Algorithm to detect Twitter bots. Given a user handle, it returns the probability of the user of being a bot. The algorithm is based on 18 heuristics, which are described below.

### Heuristic: Fake Handlers

Heuristic that allows identifying user names that contain a random string of numbers and letters or that are similar to user names used by public figures, for example @maritoabdo (true) --- current president of Paraguay---, @marioabdojunior (fake).

#### Implementation

##### Verification of similarity of one account with others

A set consisting of trustworthy accounts was created based on the number of followers of the account or if this account is verified.
With this set defined and saved in the BD, the algorithm proceeds to evaluate the similarity of the account that we wish to analyze with all the trustworthy accounts.
If the account belongs to the set of trustworthy accounts, the degree of similarity with any other will no longer be analyzed. If it is not, it will proceed to verify if the analyzed account is a derivative of a reliable account but with the suffix "junior" or "jr", in that case the probability that it is a false account will increase.
If the above cases are not applied, we proceed to compare the analyzed account ---screen_name and name--- with the trustworthy accounts using the bigram algorithm.


##### Verification of the user's name looking for random strings of numbers or letters

###### Verification of random numerical strings in an account

The numbers that have the screen_name of the account that is analyzed are extracted. If the account has many strings of numbers in the name, it is considered that the account may be false. Then look for possible "meanings" of such numbers as for example: year of creation of the account or possible date of birth --- in different formats ---.

###### Verification of the consonant-vowel relationship 

The number of vowels and consonants of screen_name and name is counted, and it is estimated that if the number of consonants is more than three times the vowels, the word probably does not make sense and maybe are random letters.

###### Verification of quantity of strings of letters

The screen_name and name are separated into strings only consisting of letters, if you have more than one string there are numbers or symbols in between could be a random name.