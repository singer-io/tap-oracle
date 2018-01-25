#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name

import datetime
import json
import os
import sys
import time
import collections
import itertools
from itertools import dropwhile
import copy
import ssl

import singer
import singer.metrics as metrics
import singer.schema
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from singer import metadata

LOGGER = singer.get_logger()

import cx_Oracle


REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]

def make_dsn(config):
   return cx_Oracle.makedsn('127.0.0.1', 1521, 'ORCL') 

def open_connection(config):
    dsn = make_dsn(config)
    conn = cx_Oracle.connect(config["user"], config["password"], dsn)
    return conn

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)
    warnings = []
    cursor = connection.cursor()
    cursor.execute("""
        SELECT id, foo, bar, timestamp FROM foo_bar
        """)
    for id, foo, bar, time in cursor:
        print("Values:", id, foo, bar, time)
 
    # log_server_params(connection)
    # if args.discover:
    #     do_discover(connection)
    # elif args.catalog:
    #     state = build_state(args.state, args.catalog)
    #     do_sync(connection, args.catalog, state)
    # elif args.properties:
    #     catalog = Catalog.from_dict(args.properties)
    #     state = build_state(args.state, catalog)
    #     do_sync(connection, catalog, state)
    # else:
    #     LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
