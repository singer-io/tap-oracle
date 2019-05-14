#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.common as common
import singer.metrics as metrics
import copy
import pdb
import time
import decimal
import cx_Oracle

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000


def sync_table(conn_config, stream, state, desired_columns):
   connection = orc_db.open_connection(conn_config)
   connection.outputtypehandler = common.OutputTypeHandler

   cur = connection.cursor()
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
   time_extracted = utils.now()

   stream_version = singer.get_bookmark(state, stream.tap_stream_id, 'version')
   ora_rowscn = singer.get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN')
   
   # If there was no bookmark for stream_version, it is the first time
   # this table is being sync'd, so get a new version, write to
   # state
   if stream_version is None:
      stream_version = int(time.time() * 1000)
      state = singer.write_bookmark(state,
                                    stream.tap_stream_id,
                                    'version',
                                    stream_version)
      singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
       
   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.stream,
      version=stream_version)
   singer.write_message(activate_version_message)

   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: common.prepare_columns_sql(stream, c), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table

   with metrics.record_counter(None) as counter:
      if ora_rowscn:
         LOGGER.info("Resuming Incremental replication from ORA_ROWSCN %s", ora_rowscn)
         select_sql      = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                               WHERE ORA_ROWSCN >= {}
                               ORDER BY ORA_ROWSCN ASC
                                """.format(','.join(escaped_columns),
                                           escaped_schema,
                                           escaped_table,
                                           ora_rowscn)
      else:
         select_sql      = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                               ORDER BY ORA_ROWSCN ASC""".format(','.join(escaped_columns),
                                                                 escaped_schema,
                                                                 escaped_table)

      rows_saved = 0
      LOGGER.info("select %s", select_sql)
      for row in cur.execute(select_sql):
         ora_rowscn = row[-1]
         row = row[:-1]
         record_message = common.row_to_singer_message(stream,
                                                       row,
                                                       stream_version,
                                                       desired_columns,
                                                       time_extracted)

         singer.write_message(record_message)
         state = singer.write_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN', ora_rowscn)
         rows_saved = rows_saved + 1
         if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
             singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

         counter.increment()

   cur.close()
   connection.close()
   return state

# Local Variables:
# python-indent-offset: 3
# End:
