#!/usr/bin/env bash

echo "Fetching and processing results from"
CLIENTS_NAMES=$(cat config/clients | sed  's/#.*$//;/^$/d')
echo $CLIENTS_NAMES

rm global_results_file.raw sorted_global_results_file.raw global_results_file.res

#IMPORTANT: point result.raw to the path AND output name as specified by your command line.
#For example, if you specified -o linkbench , then the raw data would be in linkbench.raw

# Let's fetch all the raw files from the clients and merge them locally: 

for client in $CLIENTS_NAMES; do
        echo $client
	ssh $client "cat ./your_oltpbench_dir/linkbench.raw" >> global_results_file.raw
done
wait

#Post process: sort, remove some header, and run the sampler.

sort --field-separator=',' --key=2 global_results_file.raw > sorted_global_results_file.raw
sed -i '/transaction/d' sorted_global_results_file.raw
./tools/plot/distribution.py 1 sorted_global_results_file.raw global_results_file.res
