import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
import tap_oracle.sync_strategies.log_miner as log_miner
import tap_oracle.sync_strategies.full_table as full_table
import decimal
import math
import pytz
import strict_rfc3339
import copy

try:
    from tests.utils import get_test_connection, get_test_conn_config, ensure_test_table, ensure_supplemental_logging, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages, insert_record, unselect_column
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages, insert_record, unselect_column


LOGGER = get_logger()

CAUGHT_MESSAGES = []
full_table.UPDATE_BOOKMARK_PERIOD = 1

def singer_write_message_no_cow(message):
    if isinstance(message, singer.RecordMessage) and message.stream != 'CHICKEN':
        raise Exception("simulated exception")
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

class CurrentlySyncing(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            table_spec_1 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                        {"name" : 'name', "type": "varchar2(250)"},
                                        {"name" : 'colour', "type": "varchar2(250)"}],
                            "name" : "CHICKEN",
                            'ROWDEPENDENCIES': True}
            ensure_test_table(table_spec_1)

            table_spec_2 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                        {"name" : 'name', "type": "varchar2(250)"},
                                        {"name" : 'colour', "type": "varchar2(250)"}],
                            "name" : "COW",
                            'ROWDEPENDENCIES': True}
            ensure_test_table(table_spec_2)
            ensure_supplemental_logging()

    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        with get_test_connection() as conn:
            conn.autocommit = True

            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])

            cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
            cow_stream = select_all_of_stream(cow_stream)
            cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)
            chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')

            cur = conn.cursor()

            cow_rec = {'NAME' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)
            chicken_rec = {'NAME' : 'fred', 'colour' : 'red'}
            insert_record(cur, 'CHICKEN', chicken_rec)

            state = {}
            #this will sync the CHICKEN but then blow up on the COW
            try:
                tap_oracle.do_sync(get_test_conn_config(), catalog, None, state)
            except Exception:
                blew_up_on_cow = True

            self.assertTrue(blew_up_on_cow)

            self.assertEqual(10, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertEqual('CHICKEN', CAUGHT_MESSAGES[3].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
            self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[6].value))

            #cow messages
            self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.SchemaMessage))
            self.assertEqual("COW", CAUGHT_MESSAGES[7].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.StateMessage))
            old_state = CAUGHT_MESSAGES[8].value
            self.assertEqual("ROOT-COW", old_state.get('currently_syncing'))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.ActivateVersionMessage))


            #run another do_sync which will resume with COW but then also do chicken
            singer.write_message = singer_write_message_ok
            CAUGHT_MESSAGES.clear()
            tap_oracle.do_sync(get_test_conn_config(), catalog, None, old_state)

            #cow messages
            self.assertEqual(12, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertEqual("ROOT-COW", singer.get_currently_syncing(CAUGHT_MESSAGES[1].value))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
            self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))

            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
            self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[5].value))

            #chicken messages
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.StateMessage))
            self.assertEqual("ROOT-CHICKEN", singer.get_currently_syncing(CAUGHT_MESSAGES[7].value))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.RecordMessage))
            self.assertEqual('CHICKEN', CAUGHT_MESSAGES[8].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.StateMessage))

            self.assertTrue(isinstance(CAUGHT_MESSAGES[10], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[11], singer.StateMessage))
            self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[11].value))


if __name__== "__main__":
    test1 = CurrentlySyncing()
    test1.setUp()
    test1.test_catalog()
