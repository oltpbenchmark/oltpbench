#!/usr/bin/env bash
echo "STARTING MULTI-OLTPBENCH ON CLIENTS:"

# get the clients names
CLIENTS_NAMES=$(cat config/clients | sed  's/#.*$//;/^$/d')
CLIENTS_NAMES=$(echo $CLIENTS_NAMES | sed 's/ /,/g')
echo $CLIENTS_NAMES

#IMPORTANT: OLTP-Bench should be installed on the same path on all clients, and your config file is deployed everywhere
#IMPORTANT: your command line should contain an output file -o
#Example: ./oltpbenchmark -b linkbench -c config/sample_linkbench_config.xml --execute=true -o linkbench
pdsh -w $CLIENTS_NAMES 'cd your_oltpbench_dir/;./oltpbenchmark your_usual_command_line'

wait
