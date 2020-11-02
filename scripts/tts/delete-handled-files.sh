input_dir=${1:-assets/remasterings}
output_dir=${2:-raudios}
extension=${3:-wav}
for file in $input_dir/*; do
	if test -f "$output_dir/$(echo $file | cut -d '/' -f3 | cut -d '.' -f1).$extension"; then
    	rm $file
	fi
done
