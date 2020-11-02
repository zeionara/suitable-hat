export input_dir=${1:-assets/remasterings}
let i=0
let total=$(ls -alh $input_dir | wc -l)
let total=total-3
for file in $input_dir/*; do
	export audio_path=raudios/$(echo $file | cut -d '/' -f3 | cut -d '.' -f1).wav
	python -m suitable-hat tts crt --input-file $file --output-file $audio_path --after-chunk-delay 2 --after-file-delay 4 --max-n-chars 500
  let i=i+1
  echo Handled $i/$total files
done
