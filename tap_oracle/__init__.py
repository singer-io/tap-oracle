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
from singer import utils, write_message, metadata, get_bookmark
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from log_miner import get_logs

LOGGER = singer.get_logger()

import cx_Oracle

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "data_length",
    "char_length",
    "character_used",
    "numeric_precision",
    "numeric_scale"
])

STRING_TYPES = set([
    'char',
    'nchar',
    'varchar',
    'varchar2',
    'nvarchar2',
    'long'
])

FLOAT_TYPES = set([
   'float',
   'real',
   'double precision',
   'binary_float',
   'binary_double'
])

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]


def build_state(old_state, catalog):
   LOGGER.info('Building new State from old state %s', old_state)

   #TODO: currently_syncing

   #TODO: check if replication keys have changed IFF we support incremental rep

   #TODO: move over SCN

   #TODO: move over version IFF we support incremental rep

   new_state = copy.deepcopy(old_state)
   return new_state

def make_dsn(config):
   return cx_Oracle.makedsn(config["host"], config["port"], 'ORCL')

def open_connection(config):
    conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    return conn

DEFAULT_NUMERIC_PRECISION=38
DEFAULT_NUMERIC_SCALE=0

def schema_for_column(c, pks_for_table):
   data_type = c.data_type.lower()
   result = Schema()

   # if c.table_name == 'CHICKEN':
   #    pdb.set_trace()

   numeric_scale = c.numeric_scale or DEFAULT_NUMERIC_SCALE
   numeric_precision = c.numeric_precision or DEFAULT_NUMERIC_PRECISION

   if data_type == 'number' and numeric_scale <= 0:
      if c.column_name in pks_for_table:
         result.type = ['integer']
      else:
         result.type = ['null', 'integer']

      result.minimum = -1 * (10**numeric_precision - 1)
      result.maximum = (10**numeric_precision - 1)

      if numeric_scale < 0:
         result.multipleOf = -10 * numeric_scale
      return result

   elif data_type == 'number':
      if c.column_name in pks_for_table:
         result.type = ['number']
      else:
         result.type = ['null', 'number']

      result.exclusiveMaximum = True
      result.maximum = 10 ** (numeric_precision - numeric_scale)
      result.multipleOf = 10 ** (0 - numeric_scale)
      result.exclusiveMinimum = True
      result.minimum = -10 ** (numeric_precision - numeric_scale)
      return result

   elif data_type == 'date' or data_type.startswith("timestamp"):
      # if c.table_name == 'CHICKEN':
      #    pdb.set_trace()

      if c.column_name in pks_for_table:
         result.type = ['string']
      else:
         result.type = ['null', 'string']

      result.format = 'date-time'
      return result


   elif data_type in FLOAT_TYPES:
      if c.column_name in pks_for_table:
         result.type = ['number']
      else:
         result.type = ['null', 'number']

      return result

   elif data_type in STRING_TYPES:
      character_used = c.character_used

      if c.column_name in pks_for_table:
         result.type = ['string']
      else:
         result.type = ['null', 'string']

      if character_used == 'C':
         result.maxLength = c.char_length
      return result

   return Schema(None)

def produce_row_counts(conn):
   cur = conn.cursor()
   row_counts = {}
   for row in cur.execute("""
                       SELECT table_name, num_rows
                         FROM dba_tables
                        WHERE owner != 'SYS'"""):
      row_counts[row[0]] = row[1] or 0

   return row_counts

def produce_pk_constraints(conn):
   cur = conn.cursor()
   pk_constraints = {}
   for schema, table_name, column_name in cur.execute("""
                      SELECT cols.owner, cols.table_name, cols.column_name
                       FROM all_constraints cons, all_cons_columns cols
                      WHERE cons.constraint_type = 'P'
                       AND cons.constraint_name = cols.constraint_name
                       AND cons.owner = cols.owner
                       AND cols.owner != 'SYS'"""):

     if pk_constraints.get(schema) is None:
        pk_constraints[schema] = {}

     if pk_constraints[schema].get(table_name) is None:
        pk_constraints[schema][table_name] = [column_name]
     else:
        pk_constraints[schema][table_name].append(column_name)

   return pk_constraints;


def produce_column_metadata(connection, table_schema, table_name, pk_constraints, column_schemas):
   mdata = {}

   metadata.write(mdata, (), 'key_properties', pk_constraints.get(table_schema, {}).get(table_name, []))


   for c_name in column_schemas:
      # if table_name == 'CHICKEN' and c_name == 'BAD_COLUMN':
      #    pdb.set_trace()

      if c_name in pk_constraints.get(table_schema, {}).get(table_name, []):
         metadata.write(mdata, ('properties', c_name), 'inclusion', 'automatic')
      elif column_schemas[c_name].type is None:
         metadata.write(mdata, ('properties', c_name), 'inclusion', 'unsupported')
      else:
         metadata.write(mdata, ('properties', c_name), 'inclusion', 'available')

   return mdata

def discover_columns(connection, table_info):
   cur = connection.cursor()
   cur.execute("""
                SELECT OWNER,
                       TABLE_NAME, COLUMN_NAME,
                       DATA_TYPE, DATA_LENGTH,
                       CHAR_LENGTH, CHAR_USED,
                       DATA_PRECISION, DATA_SCALE
                       from all_tab_columns
                 WHERE OWNER != 'SYS'
                 ORDER BY owner, table_name, column_name
              """)

   columns = []
   counter = 0
   rec = cur.fetchone()
   while rec is not None:
      columns.append(Column(*rec))

      rec = cur.fetchone()


   pk_constraints = produce_pk_constraints(connection)
   entries = []
   for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
      cols = list(cols)
      (table_schema, table_name) = k
      pks_for_table = pk_constraints.get(table_schema, {}).get(table_name, [])

      column_schemas = {c.column_name : schema_for_column(c, pks_for_table) for c in cols}
      schema = Schema(type='object', properties=column_schemas)

      md = produce_column_metadata(connection, table_schema, table_name, pk_constraints, column_schemas)
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

   for row in cur.execute("""
                        SELECT owner, table_name
                         FROM all_tables
                        WHERE owner != 'SYS'"""):
      schema = row[0]
      table = row[1]

      if schema not in table_info:
         table_info[schema] = {}

      is_view = False
      table_info[schema][table] = {
         'row_count': row_counts[table],
         'is_view': is_view
      }

   for row in cur.execute("""
                         SELECT owner, view_name
                          FROM sys.all_views
                         WHERE owner != 'SYS'"""):
     view_name = row[1]
     schema = row[0]
     if schema not in table_info:
        table_info[schema] = {}

     table_info[schema][view_name] = {
        'is_view': True
     }

   return discover_columns(connection, table_info)

# def update_catalog(conn, catalog, state):
#    # Filter catalog to include only selected streams
#    streams_to_sync = list(filter(lambda stream: stream.is_selected(), old_catalog.streams))

#    result = Catalog(streams=[])
#    # merge in selections from existing catalog


def should_sync_column(metadata, field_name):
   if metadata.get(('properties', field_name), {}).get('inclusion') == 'unsupported':
      return False

   if metadata.get(('properties', field_name), {}).get('selected'):
      return True

   if metadata.get(('properties', field_name), {}).get('inclusion') == 'automatic':
      return True

   return False



def fetch_current_scn(connection):
   cur = connection.cursor()
   current_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
   return current_scn

#logminer steps
# turn on
def sync_table(connection, stream, state):
   end_scn = fetch_current_scn(connection)

   stream_metadata = metadata.to_map(stream.metadata)
   desired_columns =  [c for c in stream.schema.properties.keys() if should_sync_column(stream_metadata, c)]
   desired_columns.sort()

   cur = connection.cursor()

   start_logmnr_sql = """BEGIN
                         DBMS_LOGMNR.START_LOGMNR(
                                 startScn => {},
                                 endScn => {},
                                 OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                            DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                            DBMS_LOGMNR.CONTINUOUS_MINE);
                         END;""".format(get_bookmark(state, stream.tap_stream_id, 'scn'), end_scn)

   LOGGER.info("starting LogMiner: {}".format(start_logmnr_sql))
   cur.execute(start_logmnr_sql)

   #mine changes
   cur = connection.cursor()
   mine_sql_clause = ",\n ".join(["""DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '{}.{}."{}"')""".format(stream.database, stream.table, c)
                                  for c in desired_columns])
   mine_sql = """
      SELECT OPERATION, SQL_REDO, {} from v$logmnr_contents where table_name = '{}' AND operation in ('INSERT', 'UPDATE', 'DELETE')
   """.format(mine_sql_clause, stream.table)

   LOGGER.info('mine_sql: {}'.format(mine_sql))
   for c in cur.execute(mine_sql):
      pdb.set_trace()


def do_sync(connection, catalog, state):
   streams_to_sync = list(filter(lambda stream: stream.is_selected_via_metadata(), catalog.streams))

   for stream in streams_to_sync:
      #TODO: set currently syncing:
      #state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

      schema_message = singer.SchemaMessage(stream=stream.stream,
                                             schema=stream.schema.to_dict(),
                                             key_properties=stream.key_properties,
                                             bookmark_properties=None)
      write_message(schema_message)

      with metrics.job_timer('sync_table') as timer:
         timer.tags['database'] = stream.database
         timer.tags['table'] = stream.table
         sync_table(connection, stream, state)

   return False

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
