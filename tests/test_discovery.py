import unittest
import os
import cx_Oracle, sys, string, _thread, datetime
import tap_oracle
import pdb
from singer import get_logger

DB_NAME='test_tap_oracle'

LOGGER = get_logger()

def get_test_connection():
    creds = {}
    missing_envs = [x for x in [os.getenv('TAP_ORACLE_HOST'),
                                os.getenv('TAP_ORACLE_USER'),
                                os.getenv('TAP_ORACLE_PASSWORD'),
                                os.getenv('TAP_ORACLE_PORT')] if x == None]
    if len(missing_envs) != 0:
        #pylint: disable=line-too-long
        raise Exception("set TAP_ORACLE_HOST, TAP_ORACLE_USER, TAP_ORACLE_PASSWORD, TAP_ORACLE_PORT")

    creds['host'] = os.environ.get('TAP_ORACLE_HOST')
    creds['user'] = os.environ.get('TAP_ORACLE_USER')
    creds['password'] = os.environ.get('TAP_ORACLE_PASSWORD')
    creds['port'] = os.environ.get('TAP_ORACLE_PORT')

    conn_string = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={})(PORT={}))(CONNECT_DATA=(SID=ORCL)))'.format(creds['host'], creds['port'])

    LOGGER.info("{}, {}, {}".format(creds['user'], creds['password'], conn_string))
    conn = cx_Oracle.connect(creds['user'], creds['password'], conn_string)

    return conn

def discover_catalog(connection):
    catalog = tap_oracle.do_discovery(connection)
    # catalog.streams = [s for s in catalog.streams if s.database == DB_NAME]
    return catalog


class TestSimpleTable(unittest.TestCase):

    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE chicken")
            cur.execute("CREATE TABLE chicken (name varchar(255), age integer)")


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = discover_catalog(conn)
            pdb.set_trace()




if __name__== "__main__":
    test1 = TestSimpleTable()
    test1.setUp()
    test1.test_catalog()
