# Configuration

## How to

### Use config.py

````bash
# print out benchmarks
./config.py benchmarks
# print out databases
./config.py databases
# validate yaml files
./config.py validate
# generate config using template
./config.py --bench tpcc --db mysql
````

### Run a benchmark

Use docker and docker-compose (not accurate, but easy to use)

````bash
# make sure you are in project root, NOT config folder
export BENCH=tpcc
export DB=mysql
./config/config.py validate
./config/config.py generate --bench ${BENCH} --db ${DB}
# this will start database server and create database
./docker/travis_start.sh
./oltpbenchmark --bench ${BENCH} --config config/generated_${BENCH}_${DB}_config.xml --create true --load true --execute true
````

Use local database

````bash
export BENCH=tpcc
export DB=mysql
./config/config.py validate
./config/config.py generate --bench ${BENCH} --db ${DB}
# modify config/generated_${BENCH}_${DB}_config.xml username and password to match your local config
# create database ${DB} use your local db shell
./oltpbenchmark --bench ${BENCH} --config config/generated_${BENCH}_${DB}_config.xml --create true --load true --execute true
````

### Add a new benchmark config

For instance we want to add TPCx-IoT http://www.tpc.org/tpcx-iot/default.asp

- add the config template to `config/benchmarks/sample_tpcxiot_config.xml` use mysql as target database
  - skip the rest if you don't use util scripts
- update [benchmarks.yml](benchmarks.yml), it's a human readable catalog of all supported benchmarks
  - benchmark are listed in alphabetical order
- pick the primary name for the benchmark, it should be same as your java package name, i.e. TPC-C has package `com.oltpbenchmark.benchmarks.tpcc`, thus it's primary name is `tpcc`
  - you can have alias, if you find the benchmark name too long. They will be used by util scripts when creating config and launching benchmarks, scripts will map them back to primary name before pass them to oltpbench
- add optional information, there should be at least one item to make it a valid YAML.
- run `config.py validate` to check if you have syntax error or there are name conflict (mainly due to alias)
  - use `config.py --verbose validate` if something goes wrong (no output means good)
- run `config.py benchmarks` you should see your benchmarks

````yaml
# (required) the primary name, should match what you use in java package name
tpcxiot:
  # (optional) displayname, just looks nicer
  name: TPCx-IoT
  # (optional) short description
  description: a benchmark for IoT Gateway
  # (optional) shorter names, common typos, only used by util scripts
  alias:
    - iot
  # (optional) original homepage/paper link, could be dead ...
  url: http://www.tpc.org/tpcx-iot/default.asp
  # (optional) list of contributions in time order, use github id, prefer PR over commit hash for url
  # not used by actual program, just a place to record you contribution
  contributions:
    - time: 2018-02-07
      author: at15
      url: https://github.com/oltpbenchmark/oltpbench/commit/cac6786e86869fd23ddb7813ced8357406c4aaef
````

### Add a new database config

For instance we want to add MySQL

- add the config template to `config/databases/sample_mysql_config.xml` use YCSB as target workload
  - skip the rest if you don't use util scripts
- update [databases.yml](databases.yml), it's a human readable catalog of some supported databases
  - databases are grouped by type
- pick the primary name, it will be used as `dbtype` in generated config
  - you also need to update `com.oltpbenchmark.types.DatabaseType.java` to add your database i.e. `MYSQL("com.mysql.jdbc.Driver", true, false, true),`
  - alias is same as benchmarks, used for util scripts only
- jdbc url, default template is `DB_URL_TEMPLATE = 'jdbc:{dbms}://{host}:{port}/{db}'` in [config.py](config.py)
  - {dbms} is the primary name
  - {db} is the benchmark name, i.e. `tpcc`, `tpch`
- many DBMS supports MySQL protocol, they should explicit specify their db url template, so `{dbms}` is not replaced
- `username` and `password` should match what you specified for [docker](../docker)
- `create_db` is the SQL to create database, note PostgreSQL doest not support `IF NOT EXISTS`
- `shell` is the command to open a shell to database
- `shell_exec` is the command to execute a single sql and exit, it is used for creating database on travis
- `require_native_shell` is for database that don't ship db shell in their docker image, i.e. TiDB's docker image does not have mysql shell but you can use your local mysql shell to connect to it.

````yaml
mysql:
  name: MySQL
  alias:
    - dolphin
  driver: com.mysql.jdbc.Driver
  port: 3306
  username: root
  password: oltpbenchpassword
  create_db: "CREATE DATABASE IF NOT EXISTS {db}"
  shell: "mysql -u {username} -p{password}"
  shell_exec: "mysql -u {username} -p{password} -e \"{sql}\""
````

````yaml
tidb:
  name: TiDB
  alias:
    - Tidb
  driver: com.mysql.jdbc.Driver
  port: 4000
  # NOTE: this is needed because tidb is mysql compataible
  dburl: "jdbc:mysql://{host}:{port}/{db}"
  username: root
  password: ''
  create_db: "CREATE DATABASE IF NOT EXISTS {db}"
  shell: "mysql -u {username} -h 127.0.0.1 -P 4000"
  shell_exec: "mysql -u {username} -h 127.0.0.1 -P 4000 -e \"{sql}\""
  require_native_shell: true # tidb's docker container does not ship with mysql client
````