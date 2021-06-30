#!/bin/bash

echo -ne "$OLTPBENCH_HOME./build"
for i in `ls $OLTPBENCH_HOME./lib/*.jar`; do
    # IMPORTANT: Make sure that we do not include hsqldb v1
    if [[ $i =~ .*hsqldb-1.* ]]; then
        continue
    fi
    echo -ne ":$i"
done
