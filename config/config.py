#!/usr/bin/env python3
"""
valid and generate config file based on catalog and template
"""

import os
import sys
import logging
import argparse

import yaml  # catalog are plain yaml files
import xml.etree.ElementTree as ET  # config are written in XML

NAME_PRIMARY = 1
NAME_ALIAS = 2
CATALOG_BENCHMARK_FILE = 'benchmarks.yml'
CATALOG_DATABASE_FILE = 'databases.yml'
DB_URL_TEMPLATE = 'jdbc:{dbms}://{host}:{port}/{db}'


def has_duplicate_name(entities):
    known_names = {}
    dup = False
    for name, entity in entities.items():
        if name in known_names:
            logging.error('dup: name %s already exists', name)
            dup = True
        known_names[name] = NAME_PRIMARY
        if not 'alias' in entity:
            continue
        for alias in entity['alias']:
            if alias in known_names:
                logging.error(
                    'dup: alias %s for %s already exists', name, alias)
                dup = True
            known_names[alias] = NAME_ALIAS
    return dup


def print_names(entities):
    for name, entity in entities.items():
        s = name
        if 'alias' in entity:
            s = s + ' ' + ','.join(entity['alias'])
        print(s)


def find_entity(entities, name, allow_alias=True):
    for primary_name, entity in entities.items():
        if primary_name == name:
            return entity, primary_name
        if allow_alias and 'alias' in entity:
            for alias in entity['alias']:
                if alias == name:
                    return entity, primary_name
    return None, name


def validate_benchmarks():
    logging.debug('validate %s', CATALOG_BENCHMARK_FILE)
    benchmarks = load_yaml(CATALOG_BENCHMARK_FILE)
    has_duplicate_name(benchmarks)
    return benchmarks


def validate_databases():
    logging.debug('validate %s', CATALOG_DATABASE_FILE)
    databases = load_yaml(CATALOG_DATABASE_FILE)
    has_duplicate_name(databases)
    return databases


def load_yaml(file):
    with open(file, 'r') as f:
        try:
            d = yaml.safe_load(f)
        except yaml.YAMLError:
            # TODO: the stack trace is not necessary, just need to show where the yaml is wrong
            logging.exception('invalid YAML %s', file)
            exit(1)
    return d


class ConfigUtil:
    def __init__(self):
        self.output = ''
        self.benchmark = ''
        self.database = ''
        self.scalefactor = None
        self.catalog_benchmarks = None
        self.catalog_databases = None
        self.validated = False

    def read_args(self, args):
        # TODO: allow alias
        self.benchmark = args.bench.lower()
        self.database = args.db.lower()
        self.output = args.output
        self.scalefactor = args.scalefactor

    def validate_args(self):
        # TODO: the code for benchmark and database are identical except the log message
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

    # validate load config and check if it is valid
    def validate(self):
        if self.validated:
            return
        self.catalog_benchmarks = validate_benchmarks()
        self.catalog_databases = validate_databases()
        self.validated = True

    def generate(self):
        self.validate()
        logging.debug('generate config for benchmark %s database %s',
                      self.benchmark, self.database)
        # check catalog
        self.validate_args()
        # locate the sample
        sample_benchmark_file = 'benchmarks/sample_' + self.benchmark + '_config.xml'
        if not os.path.isfile(sample_benchmark_file):
            logging.error(
                'can\'t find benchmark config template %s', sample_benchmark_file)
            exit(1)
        tree = ET.parse(sample_benchmark_file)
        root = tree.getroot()
        # update database connection
        # TODO: merge w/ command args
        db = self.catalog_databases[self.database]
        root.find('dbtype').text = self.database
        root.find('driver').text = db['driver']
        db_url = db.get('dburl', DB_URL_TEMPLATE)
        root.find('DBUrl').text = db_url.replace('{dbms}', self.database).replace(
            '{host}', 'localhost').replace('{port}', str(db['port'])).replace('{db}', self.benchmark)
        root.find('username').text = db['username']
        root.find('password').text = db['password']
        if self.scalefactor is not None:
            root.find('scalefactor').text = str(self.scalefactor)
        if self.output:
            output_file = self.output
        else:
            output_file = 'generated_{}_{}_config.xml'.format(
                self.benchmark, self.database)
        tree.write(output_file)
        print(output_file)

    def benchmarks(self):
        self.validate()
        print_names(self.catalog_benchmarks)

    def databases(self):
        self.validate()
        print_names(self.catalog_databases)


def main():
    cli = ConfigUtil()
    # https://docs.python.org/3/library/argparse.html#sub-commands
    parser = argparse.ArgumentParser(
        description='Oltpbench configuration util')
    # subcommands
    commands = parser.add_subparsers(dest='subcommand',
                                     title='subcommands',
                                     help='subcommands')
    commands.add_parser(
        'validate', help='Check catalog and config template consistency')
    commands.add_parser(
        'benchmarks', help='List supported benchmarks')
    commands.add_parser(
        'databases', help='List supported databases')
    # generate command
    cmd_gen = commands.add_parser(
        'generate', help='Generate config base on template')
    cmd_gen.add_argument('--bench', metavar='<benchmark>',
                         type=str, help='benchmark type i.e. tpcc, tpch', required=True)
    cmd_gen.add_argument('--db', metavar='<database>',
                         type=str, help='target database i.e. mysql, postgres', required=True)
    cmd_gen.add_argument('--scalefactor', metavar='<scale-factor>',
                         type=int, help='the scale factor', required=False)
    # global flags
    # NOTE: you have to apply them before sub command, i.e. config.py --verbose valid instead of config.py valid --verbose
    parser.add_argument('--verbose', dest='verbose',
                        help='verbose logging', action='store_true')
    parser.set_defaults(verbose=False)
    parser.add_argument('--output', metavar='<file>',
                        type=str, help='output file location')

    args = parser.parse_args()
    # print(args)
    if not args.subcommand:
        parser.print_help()
        exit(1)
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    # switch to directory of config.py
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    # dispatch
    if args.subcommand == 'validate':
        cli.validate()
    elif args.subcommand == 'generate':
        cli.read_args(args)
        cli.generate()
    elif args.subcommand == 'benchmarks':
        cli.benchmarks()
    elif args.subcommand == 'databases':
        cli.databases()
    else:
        print('unknown command')
        exit(1)


if __name__ == '__main__':
    main()
