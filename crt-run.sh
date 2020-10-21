for file in data/*; do
	export audio_path=crt-audios/$(echo $file | cut -d '/' -f2).wav
	python crt.py -f $file -o $audio_path
done
