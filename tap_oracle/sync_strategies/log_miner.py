#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_oracle.db as orc_db
from tap_oracle.sync_strategies.common import row_to_singer_message
import copy
import pdb

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

def fetch_current_scn(connection):
   cur = connection.cursor()
   current_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
   return current_scn

def add_automatic_properties(stream):
   stream.schema.properties['scn'] = Schema(type = ['integer'])
   stream.schema.properties['_sdc_deleted_at'] = Schema(
            type=['null', 'string'], format='date-time')


def sync_table(connection, stream, state, desired_columns, stream_version):
   cur = connection.cursor()
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T00:00:00.00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")

   end_scn = fetch_current_scn(connection)
   time_extracted = utils.now()

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
   redo_value_sql_clause = ",\n ".join(["""DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, :{})""".format(idx+1)
                                  for idx,c in enumerate(desired_columns)])

   undo_value_sql_clause = ",\n ".join(["""DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, :{})""".format(idx+1)
                                  for idx,c in enumerate(desired_columns)])

   mine_sql = """
      SELECT OPERATION, SQL_REDO, SCN, CSCN, COMMIT_TIMESTAMP,  {}, {} from v$logmnr_contents where table_name = :table_name AND operation in ('INSERT', 'UPDATE', 'DELETE')
   """.format(redo_value_sql_clause, undo_value_sql_clause)
   binds = [orc_db.fully_qualified_column_name(stream.database, stream.table, c) for c in desired_columns] + \
           [orc_db.fully_qualified_column_name(stream.database, stream.table, c) for c in desired_columns] + \
           [orc_db.quote_identifier(stream.table)]

   LOGGER.info('mine_sql: {}'.format(mine_sql))
   LOGGER.info('mine_binds: {}'.format(binds))


   state = singer.write_bookmark(state,
                                 stream.tap_stream_id,
                                 'version',
                                 stream_version)

   rows_saved = 0
   columns_for_record = desired_columns + ['scn', '_sdc_deleted_at']
   for op, redo, scn, cscn, commit_ts, *col_vals in cur.execute(mine_sql, binds):
       redo_vals = col_vals[0:len(desired_columns)]
       undo_vals = col_vals[len(desired_columns):]
       if op == 'INSERT':
             redo_vals += [cscn, None]
             record_message = row_to_singer_message(stream, redo_vals, stream_version, columns_for_record, time_extracted)

       elif op == 'UPDATE':
             redo_vals += [cscn, None]
             record_message = row_to_singer_message(stream, redo_vals, stream_version, columns_for_record, time_extracted)
       elif op == 'DELETE':
             undo_vals += [cscn, commit_ts]
             record_message = row_to_singer_message(stream, undo_vals, stream_version, columns_for_record, time_extracted)
       else:
             raise Exception("unrecognized logminer operation: {}".format(op))

       singer.write_message(record_message)
       rows_saved = rows_saved + 1
       state = singer.write_bookmark(state,
                                     stream.tap_stream_id,
                                     'scn',
                                     cscn)
       if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
             singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
