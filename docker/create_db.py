#!/usr/bin/env python3

"""
cli for creating database based on catalog
"""

import os
import argparse
import logging
import subprocess  # NOTE: we use older API because travis have Python 3.4.3

import yaml

CATALOG_BENCHMARK_FILE = '../config/benchmarks.yml'
CATALOG_DATABASE_FILE = '../config/databases.yml'

# copied from config.py


def load_yaml(file):
    with open(file, 'r') as f:
        try:
            d = yaml.safe_load(f)
        except yaml.YAMLError:
            # TODO: the stack trace is not necessary, just need to show where the yaml is wrong
            logging.exception('invalid YAML %s', file)
            exit(1)
    return d


def find_entity(entities, name, allow_alias=True):
    for primary_name, entity in entities.items():
        if primary_name == name:
            return entity, primary_name
        if allow_alias and 'alias' in entity:
            for alias in entity['alias']:
                if alias == name:
                    return entity, primary_name
    return None, name
# end of copy


class DbUtil:
    def __init__(self):
        self.benchmark = ''
        self.database = ''
        self.catalog_databases = None
        self.catalog_benchmarks = None

    def read_args(self, args):
        self.benchmark = args.bench.lower()
        self.database = args.db.lower()

    def load_catalog(self):
        self.catalog_databases = load_yaml(CATALOG_DATABASE_FILE)
        self.catalog_benchmarks = load_yaml(CATALOG_BENCHMARK_FILE)
        # handle alias, copied from config.py
        if not self.database in self.catalog_databases:
            entity, primary_name = find_entity(
                self.catalog_databases, self.database, True)
            if entity is None:
                logging.error(
                    'database %s is not in catlog, run `config.py databases` to see supported databases', self.database)
                exit(1)
            else:
                logging.debug('%s is alias of database %s',
                              self.database, primary_name)
                self.database = primary_name
        if not self.benchmark in self.catalog_benchmarks:
            entity, primary_name = find_entity(
                self.catalog_benchmarks, self.benchmark, True)
            if entity is None:
                logging.error(
                    'benchmark %s is not in catalog, run `config.py benchmarks` to see supported benchmarks',
                    self.benchmark)
                exit(1)
            else:
                logging.debug('%s is alias of benchmark %s',
                              self.benchmark, primary_name)
                self.benchmark = primary_name
        # end of copy

    def create_db(self):
        if not self.database in self.catalog_databases:
            logging.error(
                'database %s is not in catalog, check config/databases.yml or run `config/config.py benchmarks`', self.database)
            exit(1)
        db = self.catalog_databases[self.database]
        cmd = db['shell_exec']
        sql = db['create_db'].replace('{db}', self.benchmark)
        cmd = cmd.replace('{username}', db['username']).replace(
            '{password}', db['password']).replace('{sql}', sql)
        logging.debug('cmd to be executed %s', cmd)
        if 'require_native_shell' in db and db['require_native_shell'] == True:
            logging.debug('using native shell as required')
            code = subprocess.call(cmd, shell=True)
        else:
            logging.debug('using shell in docker-compose')
            code = subprocess.call([
                'docker-compose', '-f', self.database + '.yml',
                'exec', self.database, 'bash', '-c', cmd
            ])
        if code != 0:
            logging.error('non zero return code %d', code)
            exit(1)
        print('created database {} for {}'.format(
            self.benchmark, self.database))


def main():
    cli = DbUtil()
    parser = argparse.ArgumentParser(
        description='create database util')
    parser.add_argument('--bench', metavar='<benchmark>',
                        type=str, help='benchmark type i.e. tpcc, tpch', required=True)
    parser.add_argument('--db', metavar='<database>',
                        type=str, help='target database i.e. mysql, postgres', required=True)
    parser.add_argument('--verbose', dest='verbose',
                        help='verbose logging', action='store_true')
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    cli.read_args(args)
    # switch to directory of create_db.py
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    cli.load_catalog()
    cli.create_db()


if __name__ == '__main__':
    main()
