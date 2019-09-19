#!/usr/bin/env python3
import singer
import decimal
import datetime
import dateutil.parser
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
import singer.metrics as metrics
from singer.schema import Schema
import tap_oracle.db as orc_db
import copy
import pdb
import pytz
import time
import tap_oracle.sync_strategies.common as common

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

SCN_WINDOW_SIZE = None

def fetch_current_scn(conn_config):
   connection = orc_db.open_connection(conn_config)
   cur = connection.cursor()
   current_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
   cur.close()
   connection.close()
   return current_scn

def add_automatic_properties(stream):
   stream.schema.properties['scn'] = Schema(type = ['integer'])
   stream.schema.properties['_sdc_deleted_at'] = Schema(
            type=['null', 'string'], format='date-time')

   return stream

def get_stream_version(tap_stream_id, state):
   stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

   if stream_version is None:
      raise Exception("version not found for log miner {}".format(tap_stream_id))

   return stream_version

def row_to_singer_message(stream, row, version, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = stream.schema.properties[columns[idx]].type
        multiple_of = stream.schema.properties[columns[idx]].multipleOf
        format = stream.schema.properties[columns[idx]].format #date-time
        if elem is None:
            row_to_persist += (elem,)
        elif 'integer' in property_type or property_type == 'integer':
            integer_representation = int(elem)
            row_to_persist += (integer_representation,)
        elif ('number' in property_type or property_type == 'number') and multiple_of:
            decimal_representation = decimal.Decimal(elem)
            row_to_persist += (decimal_representation,)
        elif ('number' in property_type or property_type == 'number'):
            row_to_persist += (float(elem),)
        elif format == 'date-time':
            row_to_persist += (elem,)
        else:
            row_to_persist += (elem,)

    rec = dict(zip(columns, row_to_persist))
    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)

def verify_db_supplemental_log_level(connection):
   cur = connection.cursor()
   cur.execute("SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")
   result = cur.fetchone()[0]

   LOGGER.info("supplemental log level for database: %s", result)
   cur.close()
   return result == 'YES'

def verify_table_supplemental_log_level(stream, connection):
   cur = connection.cursor()
   cur.execute("""SELECT * FROM ALL_LOG_GROUPS WHERE table_name = :table_name AND LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'""", table_name = stream.table)
   result = cur.fetchone()
   LOGGER.info("supplemental log level for table(%s): %s", stream.table, result)
   cur.close()
   return result is not None

def sync_tables(conn_config, streams, state, end_scn, scn_window_size = None):
   connection = orc_db.open_connection(conn_config)
   if not verify_db_supplemental_log_level(connection):
      for stream in streams:
         if not verify_table_supplemental_log_level(stream, connection):
            raise Exception("""
      Unable to replicate with logminer for stream({}) because supplmental_log_data is not set to 'ALL' for either the table or the database.
      Please run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            """.format(stream.tap_stream_id))

   cur = connection.cursor()
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")

   start_scn_window = min([get_bookmark(state, s.tap_stream_id, 'scn') for s in streams])

   while start_scn_window < end_scn:
      stop_scn_window = end_scn
      if SCN_WINDOW_SIZE:
         stop_scn_window = start_scn_window + SCN_WINDOW_SIZE
         if stop_scn_window > end_scn:
            stop_scn_window = end_scn

      state = sync_tables_logminer(cur, streams, state, start_scn_window, stop_scn_window)

      start_scn_window = stop_scn_window

   cur.close()
   connection.close()

def sync_tables_logminer(cur, streams, state, start_scn, end_scn):

   time_extracted = utils.now()

   start_logmnr_sql = """BEGIN
                         DBMS_LOGMNR.START_LOGMNR(
                                 startScn => {},
                                 endScn => {},
                                 OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                            DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                            DBMS_LOGMNR.CONTINUOUS_MINE);
                         END;""".format(start_scn, end_scn)

   LOGGER.info("Starting LogMiner for %s: %s -> %s", list(map(lambda s: s.tap_stream_id, streams)), start_scn, end_scn)
   LOGGER.info("%s",start_logmnr_sql)
   cur.execute(start_logmnr_sql)

   #mine changes
   for stream in streams:
      md_map = metadata.to_map(stream.metadata)
      desired_columns = [c for c in stream.schema.properties.keys() if common.should_sync_column(md_map, c)]
      redo_value_sql_clause = ",\n ".join(["""DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, :{})""".format(idx+1)
                                           for idx,c in enumerate(desired_columns)])
      undo_value_sql_clause = ",\n ".join(["""DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, :{})""".format(idx+1)
                                           for idx,c in enumerate(desired_columns)])

      schema_name = md_map.get(()).get('schema-name')
      stream_version = get_stream_version(stream.tap_stream_id, state)
      mine_sql = """
      SELECT OPERATION, SQL_REDO, SCN, CSCN, COMMIT_TIMESTAMP,  {}, {} from v$logmnr_contents where table_name = :table_name AND seg_owner = :seg_owner AND operation in ('INSERT', 'UPDATE', 'DELETE')
      """.format(redo_value_sql_clause, undo_value_sql_clause)
      binds = [orc_db.fully_qualified_column_name(schema_name, stream.table, c) for c in desired_columns] + \
              [orc_db.fully_qualified_column_name(schema_name, stream.table, c) for c in desired_columns] + \
              [stream.table] + [schema_name]


      rows_saved = 0
      columns_for_record = desired_columns + ['scn', '_sdc_deleted_at']
      with metrics.record_counter(None) as counter:
         LOGGER.info("Examing log for table %s", stream.tap_stream_id)
         common.send_schema_message(stream, ['lsn'])
         for op, redo, scn, cscn, commit_ts, *col_vals in cur.execute(mine_sql, binds):
            redo_vals = col_vals[0:len(desired_columns)]
            undo_vals = col_vals[len(desired_columns):]
            if op == 'INSERT' or op == 'UPDATE':
               redo_vals += [cscn, None]
               record_message = row_to_singer_message(stream, redo_vals, stream_version, columns_for_record, time_extracted)
            elif op == 'DELETE':
               undo_vals += [cscn, singer.utils.strftime(commit_ts.replace(tzinfo=pytz.UTC))]
               record_message = row_to_singer_message(stream, undo_vals, stream_version, columns_for_record, time_extracted)
            else:
               raise Exception("unrecognized logminer operation: {}".format(op))

            singer.write_message(record_message)
            rows_saved = rows_saved + 1
            counter.increment()
            state = singer.write_bookmark(state,
                                          stream.tap_stream_id,
                                          'scn',
                                          int(cscn))


            if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
               singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   for s in streams:
      LOGGER.info("updating bookmark for stream %s to end_lsn %s", s.tap_stream_id, end_scn)
      state = singer.write_bookmark(state, s.tap_stream_id, 'scn', end_scn)
      singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   return state
