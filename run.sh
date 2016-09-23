code_dir=$(awk -F " *= *" '/code_dir/ {print $2}' config.ini)
data_dir=$(awk -F " *= *" '/data_dir/ {print $2}' config.ini)
log_dir=$(awk -F " *= *" '/log_dir/ {print $2}' config.ini)

#read from log file.
while read strline; do
	if [ -n "`echo $strline | grep "finished"`" ]; then
		#echo $strline
		date=`echo "$strline" | awk -F ' ' '{print $4}'`
		date_dir=$data_dir"/"$date"/"
		[ ! -d $date_dir ] && continue
		for fn in $(ls $date_dir); do
			[ ! -n "`echo $date_dir"/"$fn | grep ".gz"`" ] && continue
			temp_file=$date_dir"/"${fn%.gz*}
			[ -f $temp_file ] && rm -f $temp_file && echo "rm -f $temp_file"
			#echo "gzip -c -d -k -q $date_dir"/"$fn | sc_analysis_dump "
			#gzip -c -d -k -q $date_dir"/"$fn | sc_analysis_dump | python $code_dir"/"analyze_lite.py $date date
			gzip -c -d -k -q $date_dir"/"$fn | sc_analysis_dump
		done
	fi
done < $log_dir
