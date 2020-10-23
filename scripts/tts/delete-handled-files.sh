input_dir=${1:-google-data}
output_dir=${2:-converted-audios}
extension=${3:-m4a}
for file in $input_dir/*; do
	if test -f "$output_dir/$(echo $file | cut -d '/' -f2).$extension"; then
    	rm $file
	fi
done
