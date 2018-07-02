import unittest
import os
import tap_oracle
import tap_oracle.sync_strategies.full_table as full_table
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
try:
    from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_conn_config
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_conn_config

import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

CAUGHT_MESSAGES = []
COW_RECORD_COUNT = 0

def singer_write_message_bad(message):
    global COW_RECORD_COUNT

    if isinstance(message, singer.RecordMessage):
        COW_RECORD_COUNT = COW_RECORD_COUNT + 1
        if COW_RECORD_COUNT > 2:
            raise Exception("simulated exception")
        CAUGHT_MESSAGES.append(message)
    else:
        CAUGHT_MESSAGES.append(message)


def singer_write_message_ok(message):
    CAUGHT_MESSAGES.append(message)

def expected_record(fixture_row):
    expected_record = {}
    for k,v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record

def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog
full_table.UPDATE_BOOKMARK_PERIOD = 1


class FullTableInterruption(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        table_spec_1 = {"columns": [{"name": "name", "type" : "varchar(70)"},
                                    {"name" : 'age', "type": "integer "}],
                        "name" : 'COW',
                        'ROWDEPENDENCIES': True}
        ensure_test_table(table_spec_1)

        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_catalog(self):
        singer.write_message = singer_write_message_bad

        conn_config = get_test_conn_config()
        catalog = tap_oracle.do_discovery(conn_config, [])
        cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'arnold', 'age' : 10}
            insert_record(cur, 'COW', cow_rec)
            cow_rec = {'name' : 'beta', 'age' : 4}
            insert_record(cur, 'COW', cow_rec)
            chicken_rec = {'name' : 'carl', 'age' : 20}
            insert_record(cur, 'COW', chicken_rec)

            chicken_rec = {'name' : 'dannie', 'age' : 76}
            insert_record(cur, 'COW', chicken_rec)

        state = {}
        #this will only sync the 1 COW and then blew up
        try:
            tap_oracle.do_sync(get_test_conn_config(), catalog, None, state)
        except Exception as ex:
            # LOGGER.exception(ex)
            blew_up_on_cow = True

        self.assertTrue(blew_up_on_cow)

        self.assertEqual(7, len(CAUGHT_MESSAGES))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
        new_version = CAUGHT_MESSAGES[2].version

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
        self.assertEqual('COW', CAUGHT_MESSAGES[3].stream)
        self.assertEqual({'AGE': 10, 'NAME': 'arnold'}, CAUGHT_MESSAGES[3].record)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))

        #ORA_ROWSCN is set while we are processing the full table replication
        row_scn_1 = CAUGHT_MESSAGES[4].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_1)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.RecordMessage))
        self.assertEqual('COW', CAUGHT_MESSAGES[5].stream)
        self.assertEqual({'NAME': 'beta', 'AGE': 4}, CAUGHT_MESSAGES[5].record)


        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
        self.assertIsNotNone(CAUGHT_MESSAGES[6].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'))
        row_scn_2 = CAUGHT_MESSAGES[6].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_2)
        self.assertTrue(row_scn_2 > row_scn_1)
        old_state = CAUGHT_MESSAGES[6].value

        #run another do_sync
        singer.write_message = singer_write_message_ok
        blew_up_on_cow = False
        CAUGHT_MESSAGES.clear()
        tap_oracle.do_sync(get_test_conn_config(), catalog, None, old_state)

        self.assertFalse(blew_up_on_cow)
        self.assertEqual(10, len(CAUGHT_MESSAGES))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        row_scn_3 = CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_3)
        self.assertEqual(row_scn_3, row_scn_2)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
        self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)
        self.assertEqual({'NAME': 'beta', 'AGE': 4}, CAUGHT_MESSAGES[2].record)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
        row_scn_4 = CAUGHT_MESSAGES[3].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_4)
        self.assertEqual(row_scn_4, row_scn_3)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage))
        self.assertEqual('COW', CAUGHT_MESSAGES[4].stream)
        self.assertEqual({'AGE': 20, 'NAME': 'carl'}, CAUGHT_MESSAGES[4].record)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
        row_scn_5 = CAUGHT_MESSAGES[5].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_5)
        self.assertTrue(row_scn_5 > row_scn_4)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.RecordMessage))
        self.assertEqual('COW', CAUGHT_MESSAGES[6].stream)
        self.assertEqual({'AGE': 76, 'NAME': 'dannie'}, CAUGHT_MESSAGES[6].record)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.StateMessage))
        row_scn_6 = CAUGHT_MESSAGES[7].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        self.assertIsNotNone(row_scn_6)
        self.assertTrue(row_scn_6 > row_scn_5)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[8].version, new_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[9].value['currently_syncing'])



if __name__== "__main__":
    test1 = FullTableInterruption()
    test1.setUp()
    test1.test_catalog()
