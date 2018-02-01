#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name

import datetime
import pdb
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

from log_miner import get_logs

LOGGER = singer.get_logger()

import cx_Oracle


REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]

def make_dsn(config):
   return cx_Oracle.makedsn(config["host"], config["port"], 'ORCL')

def open_connection(config):
    conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    return conn

def schema_for_column(c):
   "finish me"

def produce_row_counts(conn):
   cur = conn.cursor()
   row_counts = {}
   for row in cur.execute("SELECT table_name, num_rows FROM dba_tables").fetchall():
      row_counts[row[0]] = row[1] or 0

   return row_counts

def do_discovery(connection):
   cur = connection.cursor()
   row_counts = produce_row_counts(connection)
   table_info = {}

   for row in cur.execute("SELECT owner, table_name FROM all_tables"):
      schema = row[0]
      table = row[1]

      if schema not in table_info:
         table_info[schema] = {}

      is_view = False
      table_info[schema][table] = {
         'row_count': row_counts[table],
         'is_view': is_view
      }


   for row in cur.execute("SELECT owner, view_name FROM sys.all_views"):
     view_name = row[1]
     schema = row[0]
     if schema not in table_info:
        table_info[schema] = {}

     table_info[schema][view_name] = {
        'is_view': True
     }

   pdb.set_trace()
   #cur.execute("Select * from user_tab_columns where table_name='' order by column_id")
   #cur.execute("select * from all_tables where table_name='CHICKEN'")


   #tables_sql = 'select * from all_tables where owner=:1'
   #tables_statement = 'insert into cx_pets (name, owner, type) values (:1, :2, :3)'
   #cur.execute(pet_statement, ('Big Red', sandy_id, 'horse'))
   #stuff = cur.fetchall()

   return None

def do_sync(connection, catalog, state):
   LOGGER.warn("implement me")
   return false
# start_date = args.config["start_date"]

#     logs = get_logs(args.config)

#     connection = open_connection(args.config)
#     warnings = []
#     cursor = connection.cursor()
#     cursor.execute("""
#         SELECT id, foo, bar, timestamp FROM foo_bar
#         """)
#     for id, foo, bar, time in cursor:
#         print("Values:", id, foo, bar, time)

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(connection)
    elif args.catalog:
        state = build_state(args.state, args.catalog)
        do_sync(connection, args.catalog, state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
