##--- docker ---#
.PHONY: docker-stop-all-containers
docker-stop-all-containers:
	docker stop $(shell docker ps -a -q)

.PHONY: docker-remove-all-containers
docker-remove-all-containers:
	docker rm $(shell docker ps -a -q)
##--- docker ---#

##--- database ---#
.PHONY: mysql-up
mysql-up:
	docker-compose -f mysql.yml up
.PHONY: mysql-down
mysql-down:
	docker-compose -f mysql.yml down
.PHONY: mysql-bash
mysql-bash:
	docker-compose -f mysql.yml exec mysql bash
.PHONY: mysql-db-shell
mysql-db-shell:
	docker-compose -f mysql.yml exec mysql mysql -u root -poltpbenchpassword
.PHONY: mysql-create-db
mysql-create-db:
	docker-compose -f mysql.yml exec mysql bash -c 'mysql -u root -poltpbenchpassword -e "CREATE DATABASE IF NOT EXISTS tpcc;"'

.PHONY: postgres-db-shell
postgres-db-shell:
	docker-compose -f postgres.yml exec postgres psql -U oltpbench -h 127.0.0.1

.PHONY: cassandra-db-shell
cassandra-db-shell:
	docker-compose -f cassandra.yml exec cassandra cqlsh
##--- database ---#
