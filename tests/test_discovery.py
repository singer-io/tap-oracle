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


def build_col_sql( col):
    col_sql = "{} {}".format(col['name'], col['type'])
    if col.get("identity"):
        col_sql += " GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1)"
    return col_sql

def build_table(table):
    create_sql = "CREATE TABLE {}\n".format(table['name'])
    col_sql = map(build_col_sql, table['columns'])
    pks = [c['name'] for c in table['columns'] if c.get('primary_key')]
    if len(pks) != 0:
        pk_sql = ",\n CONSTRAINT {}_pk  PRIMARY KEY({})".format(table['name'], " ,".join(pks))
    else:
       pk_sql = ""

    sql = "{} ( {} {})".format(create_sql, ",\n".join(col_sql), pk_sql)
    return sql



class TestStringTableWithPK(unittest.TestCase):

    maxDiff = None
    def setUp(self):
        table_spec = {"columns": [{"name" : "id",    "type" : "integer", "primary_key" : True, "identity" : True},
                                  {"name" : '"name-char"',  "type": "char(255)"},
                                  {"name" : '"name-nchar"',  "type": "nchar(255)"},
                                  {"name" : '"name-nvarchar2"',  "type": "nvarchar2(255)"},
                                  {"name" : '"name-varchar1"',  "type": "varchar(255)"},
                                  {"name" : '"name-varchar2"',  "type": "varchar2(255)"},
                                  {"name" : 'name_long',  "type": "long"}],
                      "name" : "CHICKEN"}
        sql = build_table(table_spec)

        with get_test_connection() as conn:
            cur = conn.cursor()
            old_table = cur.execute("select * from all_tables where owner  = '{}' AND table_name = '{}'".format("ROOT", table_spec['name'])).fetchall()
            if len(old_table) != 0:
                cur.execute("DROP TABLE {}".format(table_spec['name']))
            cur.execute(sql)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = discover_catalog(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            self.assertEqual('CHICKEN', stream_dict.get('table_name'))
            self.assertEqual(False, stream_dict.get('is_view'))
            self.assertEqual(0, stream_dict.get('row_count'))
            self.assertEqual('ROOT', stream_dict.get('database_name'))
            self.assertEqual('CHICKEN', stream_dict.get('stream'))
            self.assertEqual('ROOT-CHICKEN', stream_dict.get('tap_stream_id'))

            self.assertEqual(2, len(stream_dict.get('metadata')))
            self.assertIn({'metadata': {'key_properties': ['ID']}, 'breadcrumb': ()}, stream_dict.get('metadata'))
            self.assertIn({'metadata': {'inclusion': 'automatic'}, 'breadcrumb': ('properties', 'ID')}, stream_dict.get('metadata'))

            self.assertEqual({'properties': {'ID':             {'type': ['null', 'string']},
                                             'name-char':      {'type': ['null', 'string']},
                                             'name-nchar':     {'type': ['null', 'string']},
                                             'name-nvarchar2': {'type': ['null', 'string']},
                                             'name-varchar1':  {'type': ['null', 'string']},
                                             'name-varchar2':  {'type': ['null', 'string']},
                                             'NAME_LONG':        {'type': ['null', 'string']}},
                              'type': 'object'},  stream_dict.get('schema'))




if __name__== "__main__":
    test1 = TestStringTableWithPK()
    test1.setUp()
    test1.test_catalog()
