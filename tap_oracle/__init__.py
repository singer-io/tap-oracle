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

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale"
])

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
   data_type = c.data_type.lower()

   result = Schema(type=['null', 'string'])
   return result

def produce_row_counts(conn):
   cur = conn.cursor()
   row_counts = {}
   for row in cur.execute("SELECT table_name, num_rows FROM dba_tables").fetchall():
      row_counts[row[0]] = row[1] or 0

   return row_counts

def produce_constraints(connection):
   return {}
   # cur = conn.cursor()
   # contraints = {}
   # for row in cur.execute("SELECT table_name, num_rows FROM all_constraints").fetchall():
   #    row_counts[row[0]] = row[1] or 0

   # return row_counts


def produce_column_metadata(connection, cols, constraints):
   # inclusion = 'available'
   # if c.primary_key:
   #    inclusion = 'automatic'

   #{['AGE']:  {"inclusion" : "automatic", "primary_key" : true}}
   return {}

def discover_columns(connection, table_info):
   cur = connection.cursor()
   cur.execute("""
                SELECT OWNER,
                       TABLE_NAME, COLUMN_NAME,
                       DATA_TYPE, DATA_LENGTH,
                       DATA_PRECISION, DATA_SCALE
                       from all_tab_columns
              """)


   columns = []
   counter = 0
   rec = cur.fetchone()
   while rec is not None:
      columns.append(Column(*rec))

      rec = cur.fetchone()

   constraints = produce_constraints(connection)

   entries = []
   for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
      cols = list(cols)
      (table_schema, table_name) = k
      schema = Schema(type='object',
                      properties={c.column_name: schema_for_column(c) for c in cols})


      #{['AGE']:  {"inclusion" : "automatic", "primary_key" : true}}
      md = produce_column_metadata(connection, cols, constraints)
      entry = CatalogEntry(
         database=table_schema,
         table=table_name,
         stream=table_name,
         metadata=metadata.to_list(md),
         tap_stream_id=table_schema + '-' + table_name,
         schema=schema)

      if table_schema in table_info and table_name in table_info[table_schema]:
         entry.row_count = table_info[table_schema][table_name].get('row_count')
         entry.is_view = table_info[table_schema][table_name]['is_view']
      entries.append(entry)

   return Catalog(entries)

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

   return discover_columns(connection, table_info)

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
