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
| verison | description | generation start date |
| --- | --- | --- |
| 0.7 | Includes texts of aneks and remasterings extracted from the main community, as well as lists of people who have liked and authored them as well as complete list of unique users occurred in the data. | 23.10.2020 |
