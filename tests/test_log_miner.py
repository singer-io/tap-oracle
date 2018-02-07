import unittest
import os
import cx_Oracle, sys, string, _thread, datetime
import tap_oracle
import pdb
from singer import get_logger, write_message, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table


LOGGER = get_logger()

CAUGHT_MESSAGES = []

def singer_write_message(message):
    LOGGER.info("caught message in singer_write_message")
    CAUGHT_MESSAGES.append(message)

class MineStrings(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                begin
                    rdsadmin.rdsadmin_util.set_configuration(
                        name  => 'archivelog retention hours',
                        value => '24');
                end;
            """)

            cur.execute("""
                begin
                   rdsadmin.rdsadmin_util.alter_supplemental_logging(
                      p_action => 'ADD');
                end;
            """)

            result = cur.execute("select log_mode from v$database").fetchall()
            self.assertEqual(result[0][0], "ARCHIVELOG")



        table_spec = {"columns": [{"name" : "id", "type" : "integer", "primary_key" : True, "identity" : True},
                                  {"name" : '"name-char-explicit-byte"',  "type": "char(250 byte)"},
                                  {"name" : '"name-char-explicit-char"',  "type": "char(250 char)"},
                                  {"name" : '"name-nchar"',               "type": "nchar(123)"},
                                  {"name" : '"name-nvarchar2"',           "type": "nvarchar2(234)"},

                                  {"name" : '"name-varchar-explicit-byte"',  "type": "varchar(250 byte)"},
                                  {"name" : '"name-varchar-explicit-char"',  "type": "varchar(251 char)"},

                                  {"name" : '"name-varchar2-explicit-byte"',  "type": "varchar2(250 byte)"},
                                  {"name" : '"name-varchar2-explicit-char"',  "type": "varchar2(251 char)"},

                                  # {"name" : 'name_long',  "type": "long"},
                                  # {"name" : 'bad_column',  "type": "clob"}
        ],
                      "name" : "CHICKEN"}
        ensure_test_table(table_spec)


    def test_catalog(self):
        write_message = singer_write_message
        with get_test_connection() as conn:
            conn.autocommit = True
            catalog = tap_oracle.do_discovery(conn)
            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            selected_metadata = [{'metadata': {'selected': True}, 'breadcrumb': ()}]

            new_md = metadata.to_map(chicken_stream.metadata)


            old_md = new_md.get(())
            old_md.update({'selected': True})

            for col_name, col_schema in chicken_stream.schema.properties.items():
                old_md = new_md.get(('properties', col_name))
                old_md.update({'selected' : True})

            chicken_stream.metadatata = metadata.to_list(new_md)

            cur = conn.cursor()
            prev_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]

            LOGGER.info("previous SCN: {}".format(prev_scn))
            cur.execute("""
            INSERT INTO chicken(
                   "name-char-explicit-byte",
                   "name-char-explicit-char",
                   "name-nchar",
                   "name-nvarchar2",
                   "name-varchar-explicit-byte",
                   "name-varchar-explicit-char",
                   "name-varchar2-explicit-byte",
                   "name-varchar2-explicit-char")
            values('name-char-explicit-byte X',
                   'name-char-explicit-char X',
                   'name-nchar X',
                   'name-nvarchar2 X',
                   'name-varchar-explicit-byte X',
                   'name-varchar-explicit-char X',
                   'name-varchar2-explicit-byte X',
                   'name-varchar2-explicit-char X') """)

            post_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            LOGGER.info("post SCN: {}".format(post_scn))

            state = write_bookmark({}, chicken_stream.tap_stream_id, 'scn', prev_scn)
            messages = tap_oracle.do_sync(conn, catalog, tap_oracle.build_state(state, catalog))

            #TODO: verify that schema was emitted
            #TODO: verify that correct record was emitted

if __name__== "__main__":
    test1 = MineStrings()
    test1.setUp()
    test1.test_catalog()
