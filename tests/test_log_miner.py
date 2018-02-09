import unittest
import os
import cx_Oracle, sys, string, _thread, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream
import tap_oracle.sync_strategies.log_miner as log_miner

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



        table_spec = {"columns": [{"name" : "id",                            "type" : "integer", "primary_key" : True, "identity" : True},
                                  {"name" : '"name-char-explicit-byte"',     "type": "char(250 byte)"},
                                  {"name" : '"name-char-explicit-char"',     "type": "char(250 char)"},
                                  {"name" : 'name_nchar',                   "type": "nchar(123)"},
                                  {"name" : '"name-nvarchar2"',              "type": "nvarchar2(234)"},

                                  {"name" : '"name-varchar-explicit-byte"',  "type": "varchar(250 byte)"},
                                  {"name" : '"name-varchar-explicit-char"',  "type": "varchar(251 char)"},

                                  {"name" : '"name-varchar2-explicit-byte"', "type": "varchar2(250 byte)"},
                                  {"name" : '"name-varchar2-explicit-char"', "type": "varchar2(251 char)"}],
                      "name" : "CHICKEN"}
        ensure_test_table(table_spec)


    def test_catalog(self):

        singer.write_message = singer_write_message
        log_miner.UPDATE_BOOKMARK_PERIOD = 1

        with get_test_connection() as conn:
            conn.autocommit = True
            catalog = tap_oracle.do_discovery(conn)
            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)

            chicken_stream = set_replication_method_for_stream(chicken_stream, 'logminer')

            cur = conn.cursor()
            prev_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            cur.execute("""
            INSERT INTO chicken(
                   "name-char-explicit-byte",
                   "name-char-explicit-char",
                   name_nchar,
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
            name_nchar                     = 'name-nchar II',
            "name-nvarchar2"               = 'name-nvarchar2 II',
            "name-varchar-explicit-byte"   = 'name-varchar-explicit-byte II',
            "name-varchar-explicit-char"   = 'name-varchar-explicit-char II',
            "name-varchar2-explicit-byte"  = 'name-varchar2-explicit-byte II',
            "name-varchar2-explicit-char"  = 'name-varchar2-explicit-char II'
            WHERE
            "name-char-explicit-byte"      = 'name-char-explicit-byte I' AND
            "name-char-explicit-char"      = 'name-char-explicit-char I' AND
            name_nchar                     = 'name-nchar I' AND
            "name-nvarchar2"               = 'name-nvarchar2 I' AND
            "name-varchar-explicit-byte"   = 'name-varchar-explicit-byte I' AND
            "name-varchar-explicit-char"   = 'name-varchar-explicit-char I' AND
            "name-varchar2-explicit-byte"  = 'name-varchar2-explicit-byte I' AND
            "name-varchar2-explicit-char"  = 'name-varchar2-explicit-char I'
 """)

            cur.execute("""
            INSERT INTO chicken(
                   "name-char-explicit-byte",
                   "name-char-explicit-char",
                   name_nchar,
                   "name-nvarchar2",
                   "name-varchar-explicit-byte",
                   "name-varchar-explicit-char",
                   "name-varchar2-explicit-byte",
                   "name-varchar2-explicit-char")
            VALUES('name-char-explicit-byte III',
                   'name-char-explicit-char III',
                   'name-nchar III',
                   'name-nvarchar2 III',
                   'name-varchar-explicit-byte III',
                   'name-varchar-explicit-char III',
                   'name-varchar2-explicit-byte III',
                   'name-varchar2-explicit-char III') """)

            cur.execute(""" DELETE FROM chicken""")

            post_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            LOGGER.info("post SCN: {}".format(post_scn))

            state = write_bookmark({}, chicken_stream.tap_stream_id, 'scn', prev_scn)
            tap_oracle.do_sync(conn, catalog, tap_oracle.build_state(state, catalog))

            self.assertEqual(11, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[10], singer.StateMessage))

            #schema includes scn && _sdc_deleted_at because we selected logminer as our replication method
            self.assertEqual({"type" : ['integer']}, CAUGHT_MESSAGES[0].schema.get('properties').get('scn') )
            self.assertEqual({"type" : ['null', 'string'], "format" : "date-time"}, CAUGHT_MESSAGES[0].schema.get('properties').get('_sdc_deleted_at') )


            #verify message 1 - first insert
            rec1 = CAUGHT_MESSAGES[1].record
            self.assertIsNotNone(rec1.get('scn'))
            rec1.pop('scn')
            self.assertEqual(rec1, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte I',
                                                 'name-char-explicit-char': 'name-char-explicit-char I                                                                                                                                                                                                                                 ',
                                                 'name-nvarchar2': 'name-nvarchar2 I', 'name-varchar-explicit-char': 'name-varchar-explicit-char I',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char I',
                                                 'NAME_NCHAR': 'name-nchar I                                                                                                               ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte I                                                                                                                                                                                                                                 ',
                                                 '_sdc_deleted_at': None, 'name-varchar-explicit-byte': 'name-varchar-explicit-byte I', 'ID': '1'})


            #verify message 2 (state)
            bookmarks_1 = CAUGHT_MESSAGES[2].value.get('bookmarks')['ROOT-CHICKEN']
            self.assertIsNotNone(bookmarks_1)
            bookmarks_1_scn = bookmarks_1.get('scn')
            bookmarks_1_version = bookmarks_1.get('version')
            self.assertIsNotNone(bookmarks_1_scn)
            self.assertIsNotNone(bookmarks_1_version)

            #verify message 3 (UPDATE)
            rec2 = CAUGHT_MESSAGES[3].record
            self.assertIsNotNone(rec2.get('scn'))
            rec2.pop('scn')
            self.assertEqual(rec2, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte II',
                                                 'name-char-explicit-char': 'name-char-explicit-char II                                                                                                                                                                                                                                ',
                                                 'name-nvarchar2': 'name-nvarchar2 II', 'name-varchar-explicit-char': 'name-varchar-explicit-char II',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char II',
                                                 'NAME_NCHAR': 'name-nchar II                                                                                                              ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte II                                                                                                                                                                                                                                ',
                                                 '_sdc_deleted_at': None, 'name-varchar-explicit-byte': 'name-varchar-explicit-byte II', 'ID': '1'})


            #verify message 4 (state)
            bookmarks_2 = CAUGHT_MESSAGES[4].value.get('bookmarks')['ROOT-CHICKEN']
            self.assertIsNotNone(bookmarks_2)
            bookmarks_2_scn = bookmarks_2.get('scn')
            bookmarks_2_version = bookmarks_2.get('version')
            self.assertIsNotNone(bookmarks_2_scn)
            self.assertIsNotNone(bookmarks_2_version)
            self.assertGreater(bookmarks_2_scn, bookmarks_1_scn)
            self.assertEqual(bookmarks_2_version, bookmarks_1_version)

            #verify first DELETE message
            rec3 = CAUGHT_MESSAGES[7].record
            self.assertIsNotNone(rec3.get('scn'))
            self.assertIsNotNone(rec3.get('_sdc_deleted_at'))
            rec3.pop('scn')
            rec3.pop('_sdc_deleted_at')
            self.assertEqual(rec3, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte II',
                                                 'name-char-explicit-char': 'name-char-explicit-char II                                                                                                                                                                                                                                ',
                                                 'name-nvarchar2': 'name-nvarchar2 II', 'name-varchar-explicit-char': 'name-varchar-explicit-char II',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char II',
                                                 'NAME_NCHAR': 'name-nchar II                                                                                                              ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte II                                                                                                                                                                                                                                ',
                                                 'name-varchar-explicit-byte': 'name-varchar-explicit-byte II', 'ID': '1'})


            #verify message 10 (DELETE)
            rec4 = CAUGHT_MESSAGES[9].record
            self.assertIsNotNone(rec4.get('scn'))
            self.assertIsNotNone(rec4.get('_sdc_deleted_at'))
            rec4.pop('scn')
            rec4.pop('_sdc_deleted_at')
            self.assertEqual(rec4, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte III',
                                                 'name-char-explicit-char': 'name-char-explicit-char III                                                                                                                                                                                                                               ',
                                                 'name-nvarchar2': 'name-nvarchar2 III', 'name-varchar-explicit-char': 'name-varchar-explicit-char III',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char III',
                                                 'NAME_NCHAR': 'name-nchar III                                                                                                             ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte III                                                                                                                                                                                                                               ',
                                                 'name-varchar-explicit-byte': 'name-varchar-explicit-byte III', 'ID': '2'})



            #TODO: verify that schema was emitted
            #TODO: verify that correct record was emitted

if __name__== "__main__":
    test1 = MineStrings()
    test1.setUp()
    test1.test_catalog()
