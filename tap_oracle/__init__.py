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
from singer import utils, metadata, get_bookmark
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
import cx_Oracle
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.log_miner as log_miner
import tap_oracle.sync_strategies.full_table as full_table
LOGGER = singer.get_logger()

#LogMiner do not support LONG, LONG RAW, CLOB, BLOB, NCLOB, ADT, or COLLECTION datatypes.
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
])

FLOAT_TYPES = set([
   'binary_float',
   'binary_double'
])

REQUIRED_CONFIG_KEYS = [
    'sid',
    'host',
    'port',
    'user',
    'password',
    'default_replication_method'
]

def make_dsn(config):
   return cx_Oracle.makedsn(config["host"], config["port"], config["sid"])

def open_connection(config):
    LOGGER.info("dsn: %s", make_dsn(config))
    conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    return conn

DEFAULT_NUMERIC_PRECISION=38
DEFAULT_NUMERIC_SCALE=0

def nullable_column(col_name, col_type, pks_for_table):
   if col_name in pks_for_table:
      return  [col_type]
   else:
      return ['null', col_type]

def schema_for_column(c, pks_for_table):
   data_type = c.data_type.lower()
   result = Schema()

   numeric_scale = c.numeric_scale or DEFAULT_NUMERIC_SCALE
   numeric_precision = c.numeric_precision or DEFAULT_NUMERIC_PRECISION

   if data_type == 'number' and numeric_scale <= 0:
      result.type = nullable_column(c.column_name, 'integer', pks_for_table)
      result.minimum = -1 * (10**numeric_precision - 1)
      result.maximum = (10**numeric_precision - 1)

      if numeric_scale < 0:
         result.multipleOf = -10 * numeric_scale
      return result

   elif data_type == 'number':
      result.type = nullable_column(c.column_name, 'number', pks_for_table)

      result.exclusiveMaximum = True
      result.maximum = 10 ** (numeric_precision - numeric_scale)
      result.multipleOf = 10 ** (0 - numeric_scale)
      result.exclusiveMinimum = True
      result.minimum = -10 ** (numeric_precision - numeric_scale)
      return result

   elif data_type == 'date' or data_type.startswith("timestamp"):
      result.type = nullable_column(c.column_name, 'string', pks_for_table)

      result.format = 'date-time'
      return result

   elif data_type in FLOAT_TYPES:
      result.type = nullable_column(c.column_name, 'number', pks_for_table)
      return result

   elif data_type in STRING_TYPES:
      character_used = c.character_used
      result.type = nullable_column(c.column_name, 'string', pks_for_table)

      if character_used == 'C':
         result.maxLength = c.char_length
      return result

   #these column types are insane. they are NOT actually ieee754 floats
   #instead they are represented as decimals, but despite this
   #it appears we can say nothing about their max or min

   #"real"
   elif data_type == 'float' and c.numeric_precision == 63:
      result.type = nullable_column(c.column_name, 'number', pks_for_table)
      result.multipleOf = 10 ** -18
      return result

   #"float", "double_precision",
   elif data_type in ['float', 'double_precision']:

      result.type = nullable_column(c.column_name, 'number', pks_for_table)
      result.multipleOf = 10 ** -38
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

def get_database_name(connection):
   cur = connection.cursor()

   rows = cur.execute("SELECT name FROM v$database").fetchall()
   return rows[0][0]

def produce_column_metadata(connection, table_info, table_schema, table_name, pk_constraints, column_schemas, cols):
   mdata = {}

   table_pks = pk_constraints.get(table_schema, {}).get(table_name, [])

   #NB> sadly, some system tables like XDB$STATS have P constraints for columns that do not exist so we must protect against this
   table_pks = list(filter(lambda pk: column_schemas.get(pk, Schema(None)).type is not None, table_pks))

   database_name = get_database_name(connection)

   metadata.write(mdata, (), 'table-key-properties', table_pks)
   metadata.write(mdata, (), 'schema-name', table_schema)
   metadata.write(mdata, (), 'database-name', database_name)

   if table_schema in table_info and table_name in table_info[table_schema]:
      metadata.write(mdata, (), 'is-view', table_info[table_schema][table_name]['is_view'])

      row_count = table_info[table_schema][table_name].get('row_count')

      if row_count is not None:
         metadata.write(mdata, (), 'row-count', row_count)

   for c in cols:
      c_name = c.column_name
      metadata.write(mdata, ('properties', c_name), 'sql-datatype', c.data_type)
      if column_schemas[c_name].type is None:
         mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'unsupported')
         mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', False)
      elif c_name in pk_constraints.get(table_schema, {}).get(table_name, []):
         mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'automatic')
         mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)
      else:
         mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'available')
         mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)

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

      md = produce_column_metadata(connection,
                                   table_info,
                                   table_schema,
                                   table_name,
                                   pk_constraints,
                                   column_schemas,
                                   cols)

      entry = CatalogEntry(
         table=table_name,
         stream=table_name,
         metadata=metadata.to_list(md),
         tap_stream_id=table_schema + '-' + table_name,
         schema=schema)

      entries.append(entry)

   return Catalog(entries)

def dump_catalog(catalog):
   catalog.dump()

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

   catalog = discover_columns(connection, table_info)
   dump_catalog(catalog)
   return catalog

def should_sync_column(metadata, field_name):
   if metadata.get(('properties', field_name), {}).get('inclusion') == 'unsupported':
      return False

   if metadata.get(('properties', field_name), {}).get('selected'):
      return True

   if metadata.get(('properties', field_name), {}).get('inclusion') == 'automatic':
      return True

   return False

def send_schema_message(stream, bookmark_properties):
   schema_message = singer.SchemaMessage(stream=stream.stream,
                                         schema=stream.schema.to_dict(),
                                         key_properties=metadata.to_map(stream.metadata).get((), {}).get('table-key-properties'),
                                         bookmark_properties=bookmark_properties)
   singer.write_message(schema_message)

def is_selected_via_metadata(stream):
   table_md = metadata.to_map(stream.metadata).get((), {})
   return table_md.get('selected')

def do_sync(connection, catalog, default_replication_method, state):
   streams = list(filter(lambda stream: is_selected_via_metadata(stream), catalog.streams))
   streams.sort(key=lambda s: s.tap_stream_id)

   currently_syncing = singer.get_currently_syncing(state)

   if currently_syncing:
      streams = dropwhile(lambda s: s.tap_stream_id != currently_syncing, streams)

   for stream in streams:
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      stream_metadata = metadata.to_map(stream.metadata)

      desired_columns =  [c for c in stream.schema.properties.keys() if should_sync_column(stream_metadata, c)]
      desired_columns.sort()

      replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
      if replication_method == 'LOG_BASED':
         if get_bookmark(state, stream.tap_stream_id, 'scn'):
            log_miner.add_automatic_properties(stream)
            send_schema_message(stream, ['scn'])
            state = log_miner.sync_table(connection, stream, state, desired_columns)

         else:
            #start off with full-table replication
            end_scn = log_miner.fetch_current_scn(connection)
            send_schema_message(stream, [])
            state = full_table.sync_table(connection, stream, state, desired_columns)

            #once we are done with full table, write the scn to the state
            state = singer.write_bookmark(state, stream.tap_stream_id, 'scn', end_scn)

      elif replication_method == 'FULL_TABLE':
         send_schema_message(stream, [])
         state = full_table.sync_table(connection, stream, state, desired_columns)
      else:
         raise Exception("only LOG_BASED and FULL_TABLE are supported right now :)")

      state = singer.set_currently_syncing(state, None)
      singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)

    if args.discover:
        do_discovery(connection)
    elif args.catalog:
       state = args.state
       do_sync(connection, args.catalog, args.config.get('default_replication_method'), state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
