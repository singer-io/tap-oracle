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
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.log_miner as log_miner
import tap_oracle.sync_strategies.full_table as full_table
import tap_oracle.sync_strategies.incremental as incremental
import tap_oracle.sync_strategies.common as common
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
    'password'
]

DEFAULT_NUMERIC_PRECISION=38
DEFAULT_NUMERIC_SCALE=0

def nullable_column(col_name, col_type, pks_for_table):
   if col_name in pks_for_table:
      return  [col_type]
   else:
      return ['null', col_type]

def schema_for_column(c, pks_for_table):
   # Return Schema(None) to avoid calling lower() on a column with no datatype
   if c.data_type is None:
      LOGGER.info('Skipping column %s since it had no datatype', c.column_name)
      return Schema(None)

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

def filter_schemas_sql_clause(sql, binds_sql, owner_schema=None):
   if binds_sql:
      if owner_schema:
         return sql + """ AND {}.owner IN ({})""".format(owner_schema, ",".join(binds_sql))
      else:
         return sql + """ AND owner IN ({})""".format(",".join(binds_sql))

   else:
      return sql

def produce_row_counts(conn, filter_schemas):
   LOGGER.info("fetching row counts")
   cur = conn.cursor()
   row_counts = {}

   binds_sql = [":{}".format(b) for b in range(len(filter_schemas))]
   sql = filter_schemas_sql_clause("""
   SELECT table_name, num_rows
   FROM all_tables
   WHERE owner != 'SYS'""", binds_sql)

   for row in cur.execute(sql, filter_schemas):
      row_counts[row[0]] = row[1] or 0

   return row_counts

def produce_pk_constraints(conn, filter_schemas):
   LOGGER.info("fetching pk constraints")
   cur = conn.cursor()
   pk_constraints = {}

   binds_sql = [":{}".format(b) for b in range(len(filter_schemas))]
   sql = filter_schemas_sql_clause("""
   SELECT cols.owner, cols.table_name, cols.column_name
   FROM all_constraints cons, all_cons_columns cols
   WHERE cons.constraint_type = 'P'
   AND cons.constraint_name = cols.constraint_name
   AND cons.owner = cols.owner
   AND cols.owner != 'SYS'
   """, binds_sql, "cols")

   for schema, table_name, column_name in cur.execute(sql, filter_schemas):
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
      # Write the data_type or "None" when the column has no datatype
      metadata.write(mdata, ('properties', c_name), 'sql-datatype', (c.data_type or "None"))
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

def discover_columns(connection, table_info, filter_schemas):
   cur = connection.cursor()
   binds_sql = [":{}".format(b) for b in range(len(filter_schemas))]
   if binds_sql:
      sql = """
      SELECT OWNER,
             TABLE_NAME, COLUMN_NAME,
             DATA_TYPE, DATA_LENGTH,
             CHAR_LENGTH, CHAR_USED,
             DATA_PRECISION, DATA_SCALE
        FROM all_tab_columns
       WHERE OWNER != 'SYS' AND owner IN ({})
       ORDER BY owner, table_name, column_name
      """.format(",".join(binds_sql))
   else:
      sql = """
      SELECT OWNER,
             TABLE_NAME, COLUMN_NAME,
             DATA_TYPE, DATA_LENGTH,
             CHAR_LENGTH, CHAR_USED,
             DATA_PRECISION, DATA_SCALE
        FROM all_tab_columns
       WHERE OWNER != 'SYS'
       ORDER BY owner, table_name, column_name
      """

   LOGGER.info("fetching column info")
   cur.execute(sql, filter_schemas)

   columns = []
   counter = 0
   rec = cur.fetchone()
   while rec is not None:
      columns.append(Column(*rec))

      rec = cur.fetchone()


   pk_constraints = produce_pk_constraints(connection, filter_schemas)
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

def do_discovery(conn_config, filter_schemas):
   LOGGER.info("starting discovery")
   connection = orc_db.open_connection(conn_config)
   cur = connection.cursor()

   row_counts = produce_row_counts(connection, filter_schemas)
   table_info = {}

   binds_sql = [":{}".format(b) for b in range(len(filter_schemas))]


   sql  = filter_schemas_sql_clause("""
   SELECT owner, table_name
   FROM all_tables
   WHERE owner != 'SYS'""", binds_sql)

   LOGGER.info("fetching tables: %s %s", sql, filter_schemas)
   for row in cur.execute(sql, filter_schemas):
      schema = row[0]
      table = row[1]

      if schema not in table_info:
         table_info[schema] = {}

      is_view = False
      table_info[schema][table] = {
         'row_count': row_counts[table],
         'is_view': is_view
      }


   sql = filter_schemas_sql_clause("""
   SELECT owner, view_name
   FROM sys.all_views
   WHERE owner != 'SYS'""", binds_sql)

   LOGGER.info("fetching views")
   for row in cur.execute(sql, filter_schemas):
     view_name = row[1]
     schema = row[0]
     if schema not in table_info:
        table_info[schema] = {}

     table_info[schema][view_name] = {
        'is_view': True
     }

   catalog = discover_columns(connection, table_info, filter_schemas)
   dump_catalog(catalog)
   cur.close()
   connection.close()
   return catalog


def is_selected_via_metadata(stream):
   table_md = metadata.to_map(stream.metadata).get((), {})
   return table_md.get('selected')

#Possible state keys: replication_key, replication_key_value, version
def do_sync_incremental(conn_config, stream, state, desired_columns):
   md_map = metadata.to_map(stream.metadata)
   replication_key = md_map.get((), {}).get('replication-key')
   if not replication_key:
      raise Exception("No replication key selected for key-based incremental replication")
   LOGGER.info("Stream %s is using incremental replication with replication key %s", stream.tap_stream_id, replication_key)

   # make sure state has required keys for incremental stream
   stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)
   illegal_bk_keys = set(stream_state.keys()).difference(set(['replication_key', 'replication_key_value', 'version', 'last_replication_method']))
   if len(illegal_bk_keys) != 0:
      raise Exception("invalid keys found in state: {}".format(illegal_bk_keys))

   state = singer.write_bookmark(state, stream.tap_stream_id, 'replication_key', replication_key)

   common.send_schema_message(stream, [replication_key])
   state = incremental.sync_table(conn_config, stream, state, desired_columns)

   return state


def clear_state_on_replication_change(state, tap_stream_id, replication_key, replication_method):
   #user changed replication, nuke state
   last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
   if last_replication_method is not None and (replication_method != last_replication_method):
      state = singer.reset_stream(state, tap_stream_id)

   #key changed
   if replication_method == 'INCREMENTAL':
      if replication_key != singer.get_bookmark(state, tap_stream_id, 'replication_key'):
         state = singer.reset_stream(state, tap_stream_id)

   state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', replication_method)
   return state

def sync_method_for_streams(streams, state, default_replication_method):
   lookup = {}
   traditional_streams = []
   logical_streams = []

   for stream in streams:
      stream_metadata = metadata.to_map(stream.metadata)
      replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
      replication_key = stream_metadata.get((), {}).get('replication-key')

      state = clear_state_on_replication_change(state, stream.tap_stream_id, replication_key, replication_method)

      if replication_method not in set(['LOG_BASED', 'FULL_TABLE', 'INCREMENTAL']):
         raise Exception("Unrecognized replication_method {}".format(replication_method))

      if replication_method == 'LOG_BASED' and stream_metadata.get((), {}).get('is-view'):
         raise Exception('LogMiner is NOT supported for views. Please change the replication method for {}'.format(stream.tap_stream_id))

      md_map = metadata.to_map(stream.metadata)
      desired_columns = [c for c in stream.schema.properties.keys() if common.should_sync_column(md_map, c)]
      desired_columns.sort()

      if len(desired_columns) == 0:
         LOGGER.warning('There are no columns selected for stream %s, skipping it', stream.tap_stream_id)
         continue

      if replication_method == 'FULL_TABLE':
         lookup[stream.tap_stream_id] = 'full'
         traditional_streams.append(stream)
      elif replication_method == 'LOG_BASED':
         if not get_bookmark(state, stream.tap_stream_id, 'scn'):
            #initial full-table phase of LogMiner
            lookup[stream.tap_stream_id] = 'log_initial'
            traditional_streams.append(stream)

         elif get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN') and get_bookmark(state, stream.tap_stream_id, 'scn'):
            #finishing previously interrupted full-table (first stage of logical replication)
            lookup[stream.tap_stream_id] = 'log_initial_interrupted'
            traditional_streams.append(stream)

         #inconsistent state
         elif get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN') and not get_bookmark(state, stream.tap_stream_id, 'scn'):
            raise Exception("ORA_ROWSCN found(%s) in state implying log inintial full-table replication but no scn is present")

         else:
            #initial stage of LogMiner(full-table) has been completed. moving onto pure LogMiner
            lookup[stream.tap_stream_id] = 'pure_log'
            logical_streams.append(stream)
      else:
         # Incremental replication
         lookup[stream.tap_stream_id] = 'incremental'
         traditional_streams.append(stream)

   return lookup, traditional_streams, logical_streams

def sync_log_miner_streams(conn_config, log_miner_streams, state, end_scn):
   if log_miner_streams:
      log_miner_streams = list(map(log_miner.add_automatic_properties, log_miner_streams))
      state = log_miner.sync_tables(conn_config, log_miner_streams, state, end_scn)

   return state

def sync_traditional_stream(conn_config, stream, state, sync_method, end_scn):
   LOGGER.info("Beginning sync of stream(%s) with sync method(%s)", stream.tap_stream_id, sync_method)
   md_map = metadata.to_map(stream.metadata)
   desired_columns = [c for c in stream.schema.properties.keys() if common.should_sync_column(md_map, c)]
   desired_columns.sort()
   if len(desired_columns) == 0:
      LOGGER.warning('There are no columns selected for stream %s, skipping it', stream.tap_stream_id)
      return state

   if sync_method == 'full':
      LOGGER.info("Stream %s is using full_table replication", stream.tap_stream_id)
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      common.send_schema_message(stream, [])
      if md_map.get((), {}).get('is-view'):
         state = full_table.sync_view(conn_config, stream, state, desired_columns)
      else:
         state = full_table.sync_table(conn_config, stream, state, desired_columns)
   elif sync_method == 'log_initial':
      #start off with full-table replication
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      LOGGER.info("stream %s is using log_miner. will use full table for first run", stream.tap_stream_id)

      state = singer.write_bookmark(state, stream.tap_stream_id, 'scn', end_scn)

      common.send_schema_message(stream, [])
      state = full_table.sync_table(conn_config, stream, state, desired_columns)
   elif sync_method == 'log_initial_interrupted':
      LOGGER.info("Initial stage of full table sync was interrupted. resuming...")
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      common.send_schema_message(stream, [])
      state = full_table.sync_table(conn_config, stream, state, desired_columns)
   elif sync_method == 'incremental':
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      state = do_sync_incremental(conn_config, stream, state, desired_columns)

   else:
      raise Exception("unknown sync method {} for stream {}".format(sync_method, stream.tap_stream_id))

   state = singer.set_currently_syncing(state, None)
   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
   return state

def any_logical_streams(streams, default_replication_method):
    for stream in streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        if replication_method == 'LOG_BASED':
            return True

    return False

def do_sync(conn_config, catalog, default_replication_method, state):
   currently_syncing = singer.get_currently_syncing(state)
   streams = list(filter(is_selected_via_metadata, catalog.streams))
   streams.sort(key=lambda s: s.tap_stream_id)
   LOGGER.info("Selected streams: %s ", list(map(lambda s: s.tap_stream_id, streams)))

   if any_logical_streams(streams, default_replication_method):
      LOGGER.info("Use of log_miner requires fetching current scn...")
      end_scn = log_miner.fetch_current_scn(conn_config)
      LOGGER.info("End SCN: %s ", end_scn)
   else:
      end_scn = None

   sync_method_lookup, traditional_streams, logical_streams = sync_method_for_streams(streams, state, default_replication_method)

   if currently_syncing:
      LOGGER.info("found currently_syncing: %s", currently_syncing)
      currently_syncing_stream = list(filter(lambda s: s.tap_stream_id == currently_syncing, traditional_streams))
      if currently_syncing_stream is None:
         LOGGER.warning("unable to locate currently_syncing(%s) amongst selected traditional streams(%s). will ignore", currently_syncing, list(map(lambda s: s.tap_stream_id, traditional_streams)))
      else:
         other_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, traditional_streams))
         traditional_streams = currently_syncing_stream + other_streams
   else:
      LOGGER.info("No currently_syncing found")

   for stream in traditional_streams:
      state = sync_traditional_stream(conn_config, stream, state, sync_method_lookup[stream.tap_stream_id], end_scn)

   state = sync_log_miner_streams(conn_config, list(logical_streams), state, end_scn)
   return state

def main_impl():
   args = utils.parse_args(REQUIRED_CONFIG_KEYS)
   conn_config = {'user': args.config['user'],
                  'password': args.config['password'],
                  'host': args.config['host'],
                  'port': args.config['port'],
                  'sid':  args.config['sid']}

   if args.config.get('scn_window_size'):
      log_miner.SCN_WINDOW_SIZE=int(args.config['scn_window_size'])
   if args.discover:
      filter_schemas_prop = args.config.get('filter_schemas')
      filter_schemas = []
      if args.config.get('filter_schemas'):
         filter_schemas = args.config.get('filter_schemas').split(',')
      do_discovery(conn_config, filter_schemas)

   elif args.catalog:
      state = args.state
      do_sync(conn_config, args.catalog, args.config.get('default_replication_method'), state)
   else:
      LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
