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
