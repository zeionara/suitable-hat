for file in data/*; do
	export audio_path=audios/$(echo $file | cut -d '/' -f2).mp3
	export converted_audio_path=converted-audios/$(echo $file | cut -d '/' -f2).m4a
	python main.py -f $file -o $audio_path
	ffmpeg -i $audio_path -filter:a "atempo=1.5" -q:a 100 $converted_audio_path
done
