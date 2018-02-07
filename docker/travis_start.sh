#!/usr/bin/env bash

set -e

# switch folder http://stackoverflow.com/questions/4774054/reliable-way-for-a-bash-script-to-get-the-full-path-to-itself
pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null
ORIGINAL_WD=${PWD}
cd ${SCRIPTPATH}


docker-compose -f ${DB}.yml up -d

if [ "$DB" == 'mysql' ]; then
    sleep 10
elif [ "$DB" == 'cassandra' ]; then
    sleep 15
else
    sleep 5
fi

./create_db.py --bench ${BENCH} --db ${DB}

cd ${ORIGINAL_WD}