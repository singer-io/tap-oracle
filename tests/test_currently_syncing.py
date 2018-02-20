import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages, insert_record, unselect_column
import tap_oracle.sync_strategies.log_miner as log_miner
import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

CAUGHT_MESSAGES = []

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

class CurrentlySyncing(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            table_spec_1 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                        {"name" : 'name', "type": "varchar2(250)"},
                                        {"name" : 'colour', "type": "varchar2(250)"}],
                          "name" : "CHICKEN"}
            ensure_test_table(table_spec_1)

            table_spec_2 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                        {"name" : 'name', "type": "varchar2(250)"},
                                        {"name" : 'colour', "type": "varchar2(250)"}],
                            "name" : "COW"}
            ensure_test_table(table_spec_2)

            # table_spec_3 = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
            #                             {"name" : 'name', "type": "varchar2(250)"},
            #                             {"name" : 'colour', "type": "varchar2(250)"}],
            #                 "name" : "TIGER"}
            # ensure_test_table(table_spec_3)


    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        with get_test_connection() as conn:
            conn.autocommit = True

            catalog = tap_oracle.do_discovery(conn)

            cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
            cow_stream = select_all_of_stream(cow_stream)
            cow_stream = set_replication_method_for_stream(cow_stream, 'full_table')

            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)
            chicken_stream = set_replication_method_for_stream(chicken_stream, 'full_table')

            # tiger_stream = [s for s in catalog.streams if s.table == 'TIGER'][0]
            # tiger_stream = select_all_of_stream(tiger_stream)
            # tiger_stream = set_replication_method_for_stream(tiger_stream, 'full_table')

            cur = conn.cursor()

            cow_rec = {'NAME' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)
            chicken_rec = {'NAME' : 'fred', 'colour' : 'red'}
            insert_record(cur, 'CHICKEN', chicken_rec)
            # tiger_rec = {'NAME' : 'tony', 'colour' : 'turquoise'}
            # insert_record(cur, 'TIGER', tiger_rec)


            state = {}
            #this will sync the CHICKEN but then blow up on the COW
            try:
                tap_oracle.do_sync(conn, catalog, tap_oracle.build_state(state, catalog))
            except:
                blew_up_on_cow = True

            self.assertTrue(blew_up_on_cow)
            self.assertEqual(8, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertEqual('CHICKEN', CAUGHT_MESSAGES[3].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))

            #cow messages
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.SchemaMessage))
            self.assertEqual("COW", CAUGHT_MESSAGES[5].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
            old_state = CAUGHT_MESSAGES[6].value
            self.assertEqual("ROOT-COW", old_state.get('currently_syncing'))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.ActivateVersionMessage))


            #run another do_sync
            singer.write_message = singer_write_message_ok
            CAUGHT_MESSAGES.clear()
            tap_oracle.do_sync(conn, catalog, tap_oracle.build_state(old_state, catalog))

            self.assertEqual(4, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
            self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.ActivateVersionMessage))

            currently_syncing = state.get('currently_syncing')
            self.assertEqual(None, currently_syncing)




if __name__== "__main__":
    test1 = CurrentlySyncing()
    test1.setUp()
    test1.test_catalog()
