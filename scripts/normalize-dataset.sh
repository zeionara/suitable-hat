if [ -z $2 ]; then
	extension=$(echo $1 | rev | cut -d '.' -f1 | rev)
	output_file=$(echo $1 | rev | cut -d '.' -f2- | rev)-normalized.$extension
else
	output_file=$2
fi
cat $1 | grep -v has-text | awk '{ print $1 " " $3 " " $2}' | sed 's/ /\t/g' > $output_file
