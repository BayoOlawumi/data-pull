#! /usr/bin/bash

# Capture settings on credentials
source /home/serg/Desktop/tetfund/data-pull/credentials.env


diff(){
  awk 'BEGIN{RS=ORS=" "}
       {NR==FNR?a[$0]++:a[$0]--}
       END{for(k in a)if(a[k])print k}' <(echo -n "${!1}") <(echo -n "${!2}")
}
# Log in to Slave

export PGPASSWORD=$SLAVE_PASSWORD
streaming_credentials="-h $SLAVE_HOST -U $SLAVE_USER -p $SLAVE_PORT -d $SLAVE_DB"	
streamed_id=($( psql $streaming_credentials < "/home/serg/Desktop/tetfund/data-pull/sql_one.sql" | grep -o '[0-9]*' )) 

# Log In to Storage
export PGPASSWORD=$ST_PASSWORD
st_credentials="-h $ST_HOST -U $ST_USER -p $ST_PORT -d $ST_DB"
stored_id=($( psql $st_credentials < "/home/serg/Desktop/tetfund/data-pull/sql_one.sql" | grep -o '[0-9]*' ))


# Get difference in ID
#filtered_id=($(comm -3 <(printf "%s\n" "${streamed_id[@]}" | sort) <(printf "%s\n" "${stored_id[@]}" | sort) | sort -n))

id_filtered=($(diff streamed_id[@] stored_id[@]))
sorted_id=( $( printf "%s\n" "${id_filtered[@]}" | sort -n) )


echo "$dash_line" >>/home/serg/Desktop/tetfund/data-pull/story_log.log
echo "Logging was attempted $(date '+%Y-%m-%d %H:%M:%S')" >>/home/serg/Desktop/tetfund/data-pull/story_log.log
echo $dash_line

# Remove the first on, often try to repeat the first element
unset sorted_id[0]

if [ "${#sorted_id[@]}" == 0 ]
then
	echo "No logging was done as at $(date '+%Y-%m-%d %H:%M:%S') as no update was found" >>/home/serg/Desktop/tetfund/data-pull/story_log.log
else
	# log data into storage
	node_c_ahead_by=()
	for id in "${sorted_id[@]}"
	do
		# Login to the Streaming Node to collect data linked to a specific id
		export PGPASSWORD=$SLAVE_PASSWORD
		obtained=$( psql $streaming_credentials -c "\pset tuples_only" -c "SELECT * FROM power_energydata WHERE id=${id}")
		
		# Clean the captured data point and extract relevant data
		# rem_arr stands for the refined data in the array format
		arr=(${obtained//|/ })
		rem_arr=( "${arr[@]:4}" )
				
		rem_arr[2]+=" ${rem_arr[3]}"
		rem_arr[9]+=" ${rem_arr[10]}"
		unset rem_arr[3]
		unset rem_arr[10]
		unset rem_arr[3]
		
		
		# If Node C is ahead of Node B with certain IDs should some IDs have been cleared from Node B
		if [ -z "${rem_arr[0]}" ]
		then
			node_c_ahead_by+=("$id")
			continue
		fi
		
		# Log In to Storage
		dat_id="${rem_arr[0]}"
		meter_id="${rem_arr[1]}"
		timestamp="${rem_arr[2]}"
		current="${rem_arr[4]}"
		voltage="${rem_arr[5]}"
		frequency="${rem_arr[6]}"
		power_factor="${rem_arr[7]}"
		energy="${rem_arr[8]}"
		w_timestamp="${rem_arr[9]}"
		
		# Login to the Storage Node to Save data point
		export PGPASSWORD=$ST_PASSWORD
		response=$( psql $st_credentials -c "INSERT INTO power_energydata (id, meter_id, real_timestamp, current, voltage, frequency, power_factor, energy, w_timestamp) VALUES ('${dat_id}', '${meter_id}', '${timestamp}', '${current}', '${voltage}', '${frequency}', '${power_factor}', '${energy}','${w_timestamp}')" )
		if [ "$response" = "INSERT 0 1" ]
		then
			echo "$id was successfully logged from Node B to Node C"
			last_successful_id=$dat_id
		else
			echo "error logging $id"
			continue
		fi
	done
	
	# Print out the IDs of Node C that are ahead Node B and Node C 
	if [ ${#node_c_ahead_by[@]} -gt 0 ]
	then
		echo "Logging ended $(date '+%Y-%m-%d %H:%M:%S') as Node C is ahead of Node B by ${sorted_id[@]} " >>/home/serg/Desktop/tetfund/data-pull/story_log.log
		
	fi
	
	# In case we have something ahead of Node C, this prints out the last updated ID
	if [ ! -z "$last_successful_id" ]
	then
		echo " Logging ended $(date '+%Y-%m-%d %H:%M:%S') with $last_successful_id as the last sucessfully logged ID" >>/home/serg/Desktop/tetfund/data-pull/story_log.log
	fi
fi

