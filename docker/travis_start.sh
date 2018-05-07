#!/usr/bin/env bash

set -e

# switch folder http://stackoverflow.com/questions/4774054/reliable-way-for-a-bash-script-to-get-the-full-path-to-itself
pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null
ORIGINAL_WD=${PWD}
cd ${SCRIPTPATH}


docker-compose -f ${DB}.yml up -d

echo 'sleep to wait for db server boostrap'

if [ "$DB" == 'mysql' ]; then
    sleep 10
elif [ "$DB" == 'memsql' ]; then
    sleep 30
elif [ "$DB" == 'cassandra' ]; then
    sleep 15
else
    sleep 5
fi

./create_db.py --bench ${BENCH} --db ${DB}

echo 'database started using docker-compse and db created'

cd ${ORIGINAL_WD}