import unittest
import os
import cx_Oracle, sys, string, _thread, datetime
import tap_oracle
import pdb
from singer import get_logger, write_message, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream


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
            chicken_stream = select_all_of_stream(chicken_stream)

            cur = conn.cursor()
            prev_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
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
            VALUES('name-char-explicit-byte I',
                   'name-char-explicit-char I',
                   'name-nchar I',
                   'name-nvarchar2 I',
                   'name-varchar-explicit-byte I',
                   'name-varchar-explicit-char I',
                   'name-varchar2-explicit-byte I',
                   'name-varchar2-explicit-char I') """)

            cur.execute("""
            UPDATE chicken
            SET
            "name-char-explicit-byte" = 'name-char-explicit-byte II',
            "name-char-explicit-char"      = 'name-char-explicit-char II',
            "name-nchar"                   = 'name-nchar II',
            "name-nvarchar2"               = 'name-nvarchar2 II',
            "name-varchar-explicit-byte"   = 'name-varchar-explicit-byte II',
            "name-varchar-explicit-char"   = 'name-varchar-explicit-char II',
            "name-varchar2-explicit-byte"  = 'name-varchar2-explicit-byte II',
            "name-varchar2-explicit-char"  = 'name-varchar2-explicit-char II'
            WHERE
            "name-char-explicit-byte"      = 'name-char-explicit-byte I' AND
            "name-char-explicit-char"      = 'name-char-explicit-char I' AND
            "name-nchar"                   = 'name-nchar I' AND
            "name-nvarchar2"               = 'name-nvarchar2 I' AND
            "name-varchar-explicit-byte"   = 'name-varchar-explicit-byte I' AND
            "name-varchar-explicit-char"   = 'name-varchar-explicit-char I' AND
            "name-varchar2-explicit-byte"  = 'name-varchar2-explicit-byte I' AND
            "name-varchar2-explicit-char"  = 'name-varchar2-explicit-char I'
 """)

            cur.execute("""
             DELETE FROM chicken""")

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
