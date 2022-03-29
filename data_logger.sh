#! /usr/bin/bash

# This bash file ensures the server is ready to log data

# Capture settings on credentials
#source credentials.env


min_ram="2000"
disk_usage="80"
dash_line='-------------------------------------------------------------------------------'
# Functions 
# Necessary Function
error_function () { while IFS='' read -r line; do echo "$(date '+%Y-%m-%d %H:%M:%S') $line" >> /home/serg/Desktop/tetfund/data-pull/logging_error.log; done; };


available_ram ()
{
	# Get the available space on the RAM
	local aval_ram=$(cat /proc/meminfo | grep -i 'MemAvailable' | grep -o '[[:digit:]]*')
	echo $aval_ram
}

available_disk_size ()
{
	# Get available space of the disk
	local usage=$(df -h | grep -i '/dev/sda5' | awk '{ print $5 }' | cut -d'%' -f1)
	echo $usage
}



# Main code
# Capture values from the functions
exec 2> >(error_function)
av_ram=$(available_ram)
av_disk=$(available_disk_size)

# Subject the returned values to Limit
if (($av_ram < $min_ram))
then
	echo $dash_line >&2
	echo "RAM is less than 2GB, data logging is postponed!" >&2
elif (($av_disk > $disk_usage))
then
	echo $dash_line >&2
	echo "Disk Space Used is $av_disk%, More memory space is needed. Data logging is postponed!"  >&2
# Data is only logged to Node C when the server is in a good state
else
	# Enroutew to the shell that logs the data
	source /home/serg/Desktop/tetfund/data-pull/real_logger.sh
fi

