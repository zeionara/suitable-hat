# Installation
```shell script
conda env create -f calendar-claimer/environment.yml
```
# Usage
To parse aneks from a vk community:
```shell script
python -m suitable-hat load
```
To merge content of multiple cache files with aneks into a single txt file (one anek per row):
```shell script
python -m suitable-hat join
```
To convert text to speech using google translate api:
```shell script
python -m suitable-hat tts google --after-file-delay 0
python -m suitable-hat tts google --after-file-delay 0 --input-file assets/baneks/000000.txt --output-file anek.mp3 --max-n-chars 100
```
# Corpora
Collected corpora are available via [this link](https://bit.ly/baneks-corpora) and are presented in the following variations:  
| verison | description | generation start date | formats |
| --- | --- | --- | --- |
| 00.07.00 | Includes texts of aneks and remasterings extracted from the main community, as well as lists of people who have liked and authored them as well as complete list of unique users occurred in the data. | 23.10.2020 | yml, binary |
| 00.07.01 | Added aneks from the two additional communities (baneksbest and anekdotikategoriib) | 23.10.2020 | binary, text (triples) |
| 00.08.00 | Added users (their ids, friends and subscriptions) | 23.10.2020 | binary (users, zipped), text (triples, zipped) |
| 00.08.01 | Converted to ttl format | 23.10.2020 | ttl (zipped) |
| 00.09.00 | Redesigned the shape of the dataset (changed way of generating node identifiers so as in the newer version original values are taken whenever possible + community identifier + node type). Does not contain data about users (precisely, their lists of friends and subscriptions). | 04.11.2020 | binary, text (triples, zipped) |
| 00.09.01 | Added users (friends and subscriptions) | 04.11.2020 | binary (users, zipped), text (triples, zipped) |
| 00.09.02 | Eliminated 'None' users (they were associated with aneks which were created by an owning community, not a particular user) | 04.11.2020 | text (triples, zipped) |
| 00.09.03 | Converted to ttl format | 04.11.2020 | ttl (zipped) |
