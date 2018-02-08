#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
import tap_oracle.db as orc_db
from tap_oracle.sync_strategies.common import row_to_singer_message

LOGGER = singer.get_logger()

def fetch_current_scn(connection):
   cur = connection.cursor()
   current_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
   return current_scn


def sync_table(connection, stream, state, desired_columns, stream_version):
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
      SELECT OPERATION, SQL_REDO, {}, {} from v$logmnr_contents where table_name = :table_name AND operation in ('INSERT', 'UPDATE', 'DELETE')
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

   for op, redo, *col_vals in cur.execute(mine_sql, binds):
       redo_vals = col_vals[0:len(desired_columns)]
       undo_vals = col_vals[len(desired_columns):]
       if op == 'INSERT':
             record_message = row_to_singer_message(stream, redo_vals, stream_version, desired_columns, time_extracted)
             singer.write_message(record_message)
       elif op == 'UPDATE':
             record_message = row_to_singer_message(stream, redo_vals, stream_version, desired_columns, time_extracted)
             singer.write_message(record_message)
       elif op == 'DELETE':
             "implement me"
       else:
             raise Exception("unrecognized logminer operation: {}".format(op))

       # state = singer.write_bookmark(state,
       #                               catalog_entry.tap_stream_id,
       #                               'replication_key_value',
       #                               record_message.record[replication_key])
       # if rows_saved % 1000 == 0:
       #    yield singer.StateMessage(value=copy.deepcopy(state))
       # singer.write_message(record_message)
