# merge script

nbfiles=0;
i=0;
file_name="big_file";

for f in *.html.gz; 
do
	let "nbfiles=(nbfiles+1)%2"

	if [ $nbfiles -lt 2 ]; then
		zcat "$f" >> big_file$nbfiles.html;
	fi
	
done