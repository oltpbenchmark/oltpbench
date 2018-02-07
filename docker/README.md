# Docker config to setup and run things locally

## Installation

Docker + Docker compose

- Mac https://docs.docker.com/docker-for-mac/install/
- Ubuntu
  - download `.deb` directly for ubuntu https://download.docker.com/linux/ubuntu/dists/artful/pool/stable/amd64/
    - change
  - download `docker-compose` binary, put it in your `PATH`

## Usage

Start the database

````bash
docker-compose -f mysql.yml up
# clean up
docker-compose -f mysql.yml down
````

Create a database using db shell inside the container

````bash
./create_db.py --bench=tpcc --db=mysql
# which actually runs
# docker-compose -f mysql.yml exec mysql bash -c 'mysql -u root -poltpbenchpassword -e "CREATE DATABASE IF NOT EXISTS tpcc"'
````

Run a benchmark

````bash
# inside docker folder
export BENCH=tpcc
export DB=mysql
./travis_start
../config/config.py generate --bench=${BENCH} --db=${DB}
../oltpbenchmark --bench ${BENCH} --config config/generated_${BENCH}_${DB}_config.xml --create true --load true --execute true
````

## Development

Add a new database

- add a new compose file, i.e. `mysql.yml`
- use variable to set version https://docs.docker.com/compose/compose-file/#variable-substitution
  - `.env` for setting version
  - `${VARIABLE:-default}` will evaluate to `default` if `VARIABLE` is unset or empty in the environment

## Databases

- MySQL
  - 5.7 Default
  - 8.0 won't work https://github.com/benchhub/forks/issues/2
- Postgres
  - 10.1 Default
- TiDB
- MemSQL