import unittest
import os
import cx_Oracle, sys, string, datetime
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

def singer_write_message_no_cow(message):
    global COW_RECORD_COUNT

    if isinstance(message, singer.RecordMessage) and message.stream == 'COW':
        COW_RECORD_COUNT = COW_RECORD_COUNT + 1
        if COW_RECORD_COUNT > 2:
            raise Exception("simulated exception")
        CAUGHT_MESSAGES.append(message)
    else:
        CAUGHT_MESSAGES.append(message)

def singer_write_schema_ok(message):
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

class LogicalInterruption(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                    {"name" : 'name', "type": "nvarchar2(100)"},
                                    {"name" : 'colour', "type": "nvarchar2(100)"}],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()
        tap_oracle.dump_catalog = do_not_dump_catalog
        full_table.UPDATE_BOOKMARK_PERIOD = 1


    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        conn_config = get_test_conn_config()
        catalog = tap_oracle.do_discovery(conn_config, [])
        cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'LOG_BASED')

        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'smelly', 'colour' : 'brow'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'pooper', 'colour' : 'green'}
            insert_record(cur, 'COW', cow_rec)

        state = {}
        #the initial phase of cows logical replication will be a full table.
        #it will sync the first record and then blow up on the 2nd record
        try:
            tap_oracle.do_sync(get_test_conn_config(), catalog, None, state)
        except Exception as ex:
            blew_up_on_cow = True

        self.assertTrue(blew_up_on_cow)

        self.assertEqual(7, len(CAUGHT_MESSAGES))
        # import pdb
        # pdb.set_trace()
        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertEqual(CAUGHT_MESSAGES[1].value['currently_syncing'], 'ROOT-COW')
        self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW']['version'])
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW']['last_replication_method'],
                         'LOG_BASED')

        self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'))
        self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('scn'))
        end_scn = CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('scn')
        first_version = CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('version')

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[2].version, first_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[3].record, {'NAME': 'betty', 'ID': 1, 'COLOUR': 'blue'})
        self.assertEqual('COW', CAUGHT_MESSAGES[3].stream)
        self.assertEqual(first_version, CAUGHT_MESSAGES[3].version)


        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
        #ORA_ROWSCN is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[4].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        self.assertEqual(CAUGHT_MESSAGES[4].value['bookmarks']['ROOT-COW']['scn'], end_scn)
        self.assertEqual(first_version, CAUGHT_MESSAGES[4].value['bookmarks']['ROOT-COW']['version'])

        self.assertEqual(CAUGHT_MESSAGES[5].record['NAME'], 'smelly')
        self.assertEqual('COW', CAUGHT_MESSAGES[5].stream)
        self.assertEqual(first_version, CAUGHT_MESSAGES[5].version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
        self.assertEqual(first_version, CAUGHT_MESSAGES[6].value['bookmarks']['ROOT-COW']['version'])
        last_ora_rowscn = CAUGHT_MESSAGES[6].value['bookmarks']['ROOT-COW']['ORA_ROWSCN']
        old_state = CAUGHT_MESSAGES[6].value


        #run another do_sync, should get the remaining record which effectively finishes the initial full_table
        #replication portion of the logical replication
        singer.write_message = singer_write_message_ok
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        CAUGHT_MESSAGES.clear()
        tap_oracle.do_sync(get_test_conn_config(), catalog, None, old_state)

        self.assertEqual(8, len(CAUGHT_MESSAGES))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'), last_ora_rowscn)
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('scn'), end_scn)
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW'].get('version'), first_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[2].record, {'COLOUR': 'brow', 'ID': 2, 'NAME': 'smelly'})
        self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
        self.assertTrue(CAUGHT_MESSAGES[3].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'), last_ora_rowscn)
        self.assertEqual(CAUGHT_MESSAGES[3].value['bookmarks']['ROOT-COW'].get('scn'), end_scn)
        self.assertEqual(CAUGHT_MESSAGES[3].value['bookmarks']['ROOT-COW'].get('version'), first_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[4].record['NAME'], 'pooper')
        self.assertEqual('COW', CAUGHT_MESSAGES[4].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
        self.assertTrue(CAUGHT_MESSAGES[5].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN') > last_ora_rowscn)
        self.assertEqual(CAUGHT_MESSAGES[5].value['bookmarks']['ROOT-COW'].get('scn'), end_scn)
        self.assertEqual(CAUGHT_MESSAGES[5].value['bookmarks']['ROOT-COW'].get('version'), first_version)


        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[6].version, first_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[7].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'))
        self.assertEqual(CAUGHT_MESSAGES[7].value['bookmarks']['ROOT-COW'].get('scn'), end_scn)
        self.assertEqual(CAUGHT_MESSAGES[7].value['bookmarks']['ROOT-COW'].get('version'), first_version)

class FullTableInterruption(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                    {"name" : 'name', "type": "nvarchar2(100)"},
                                    {"name" : 'colour', "type": "nvarchar2(100)"}],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)

        table_spec_2 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                    {"name" : 'name', "type": "nvarchar2(100)"},
                                    {"name" : 'colour', "type": "nvarchar2(100)"}],
                        "name" : 'CHICKEN'}
        ensure_test_table(table_spec_2)

        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()
        tap_oracle.dump_catalog = do_not_dump_catalog
        full_table.UPDATE_BOOKMARK_PERIOD = 1


    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        conn_config = get_test_conn_config()
        catalog = tap_oracle.do_discovery(conn_config, [])
        cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

        chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
        self.assertIsNotNone(chicken_stream)
        chicken_stream = select_all_of_stream(chicken_stream)
        chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')
        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)
            cow_rec = {'name' : 'smelly', 'colour' : 'brow'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'pooper', 'colour' : 'green'}
            insert_record(cur, 'COW', cow_rec)

            chicken_rec = {'name' : 'fred', 'colour' : 'red'}
            insert_record(cur, 'CHICKEN', chicken_rec)

        state = {}
        #this will sync the CHICKEN but then blow up on the COW
        try:
            tap_oracle.do_sync(get_test_conn_config(), catalog, None, state)
        except Exception as ex:
            # LOGGER.exception(ex)
            blew_up_on_cow = True

        self.assertTrue(blew_up_on_cow)


        self.assertEqual(14, len(CAUGHT_MESSAGES))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-CHICKEN'].get('ORA_ROWSCN'))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
        new_version = CAUGHT_MESSAGES[2].version

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
        self.assertEqual('CHICKEN', CAUGHT_MESSAGES[3].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
        #ORA_ROWSCN is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[4].value['bookmarks']['ROOT-CHICKEN']['ORA_ROWSCN'])

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[5].version, new_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
        self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[6].value))
        #ORA_ROWSCN is cleared at the end of the full table replication
        self.assertIsNone(CAUGHT_MESSAGES[6].value['bookmarks']['ROOT-CHICKEN']['ORA_ROWSCN'])


        #cow messages
        self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.SchemaMessage))

        self.assertEqual("COW", CAUGHT_MESSAGES[7].stream)
        self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[8].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN'))
        self.assertEqual("ROOT-COW", CAUGHT_MESSAGES[8].value['currently_syncing'])

        self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.ActivateVersionMessage))
        cow_version = CAUGHT_MESSAGES[9].version
        self.assertTrue(isinstance(CAUGHT_MESSAGES[10], singer.RecordMessage))

        self.assertEqual(CAUGHT_MESSAGES[10].record['NAME'], 'betty')
        self.assertEqual('COW', CAUGHT_MESSAGES[10].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[11], singer.StateMessage))
        #ORA_ROWSCN is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[11].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        betty_ora_row_scn = CAUGHT_MESSAGES[11].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN')


        self.assertEqual(CAUGHT_MESSAGES[12].record['NAME'], 'smelly')
        self.assertEqual('COW', CAUGHT_MESSAGES[12].stream)

        old_state = CAUGHT_MESSAGES[13].value
        self.assertIsNotNone(CAUGHT_MESSAGES[13].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        smelly_ora_row_scn = CAUGHT_MESSAGES[13].value['bookmarks']['ROOT-COW'].get('ORA_ROWSCN')

        self.assertGreater(smelly_ora_row_scn, betty_ora_row_scn)

        #run another do_sync
        singer.write_message = singer_write_message_ok
        CAUGHT_MESSAGES.clear()
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0

        tap_oracle.do_sync(get_test_conn_config(), catalog, None, old_state)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))

        # because we were interrupted, we do not switch versions
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW']['version'], cow_version)
        self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        self.assertEqual("ROOT-COW", singer.get_currently_syncing(CAUGHT_MESSAGES[1].value))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[2].record['NAME'], 'smelly')
        self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)


        #after record: activate version, state with no ORA_ROWSCN or currently syncing
        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
        #we still have an ORA_ROWSCN for COW because are not yet done with the COW table
        self.assertIsNotNone(CAUGHT_MESSAGES[3].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        self.assertEqual(singer.get_currently_syncing( CAUGHT_MESSAGES[3].value), 'ROOT-COW')

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[4].record['NAME'], 'pooper')
        self.assertEqual('COW', CAUGHT_MESSAGES[4].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
        self.assertIsNotNone(CAUGHT_MESSAGES[5].value['bookmarks']['ROOT-COW']['ORA_ROWSCN'])
        self.assertEqual(singer.get_currently_syncing( CAUGHT_MESSAGES[5].value), 'ROOT-COW')


        #ORA_ROWSCN is cleared because we are finished the full table replication
        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[6].version, cow_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.StateMessage))
        self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[7].value))
        self.assertIsNone(CAUGHT_MESSAGES[7].value['bookmarks']['ROOT-CHICKEN']['ORA_ROWSCN'])
        self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[7].value))


if __name__== "__main__":
    test1 = FullTableInterruption()
    test1.setUp()
    test1.test_catalog()
