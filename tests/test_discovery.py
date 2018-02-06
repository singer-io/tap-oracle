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



def ensure_test_table(table_spec):
    sql = build_table(table_spec)

    with get_test_connection() as conn:
        cur = conn.cursor()
        old_table = cur.execute("select * from all_tables where owner  = '{}' AND table_name = '{}'".format("ROOT", table_spec['name'])).fetchall()
        if len(old_table) != 0:
            cur.execute("DROP TABLE {}".format(table_spec['name']))

        cur.execute(sql)

class TestStringTableWithPK(unittest.TestCase):
    maxDiff = None
    def setUp(self):
       table_spec = {"columns": [{"name" : "id", "type" : "integer", "primary_key" : True, "identity" : True},
                                 #NLS_LENGTH_SEMANTICS = byte
                                 {"name" : '"name-char-explicit-byte"',  "type": "char(250 byte)"},
                                 {"name" : '"name-char-explicit-char"',  "type": "char(250 char)"},
                                 {"name" : '"name-nchar"',               "type": "nchar(123)"},
                                 {"name" : '"name-nvarchar2"',           "type": "nvarchar2(234)"},

                                 {"name" : '"name-varchar-explicit-byte"',  "type": "varchar(250 byte)"},
                                 {"name" : '"name-varchar-explicit-char"',  "type": "varchar(251 char)"},

                                 {"name" : '"name-varchar2-explicit-byte"',  "type": "varchar2(250 byte)"},
                                 {"name" : '"name-varchar2-explicit-char"',  "type": "varchar2(251 char)"},

                                 {"name" : 'name_long',  "type": "long"},
                                 {"name" : 'bad_column',  "type": "clob"}],
                      "name" : "CHICKEN"}
       ensure_test_table(table_spec)

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


            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual(stream_dict.get('metadata'),
                             [{'metadata': {'key_properties': ['ID']}, 'breadcrumb': ()},
                              {'metadata': {'inclusion': 'unsupported'}, 'breadcrumb': ('properties', 'BAD_COLUMN')},
                              {'metadata': {'inclusion': 'automatic'}, 'breadcrumb': ('properties', 'ID')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'NAME_LONG')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-char-explicit-byte')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-char-explicit-char')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-nchar')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-nvarchar2')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-varchar-explicit-byte')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-varchar-explicit-char')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-varchar2-explicit-byte')},
                              {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'name-varchar2-explicit-char')}])

            self.assertEqual({'properties': {'ID':                      {'type': ['integer'],
                                                                         'maximum': 99999999999999999999999999999999999999,
                                                                         'minimum': -99999999999999999999999999999999999999},
                                             'BAD_COLUMN':              {},
                                             'name-char-explicit-byte': {'type': ['null', 'string']},
                                             'name-char-explicit-char': {'type': ['null', 'string'], 'maxLength': 250},

                                             'name-nchar':     {'type': ['null', 'string'], 'maxLength': 123 },
                                             'name-nvarchar2': {'type': ['null', 'string'], 'maxLength': 234 },

                                             'name-varchar-explicit-byte': {'type': ['null', 'string']},
                                             'name-varchar-explicit-char': {'type': ['null', 'string'], 'maxLength': 251},

                                             'name-varchar2-explicit-byte': {'type': ['null', 'string']},
                                             'name-varchar2-explicit-char': {'type': ['null', 'string'], 'maxLength': 251},

                                             'NAME_LONG':        {'type': ['null', 'string']}},
                              'type': 'object'},  stream_dict.get('schema'))


class TestIntegerTableNoPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [# {"name" : "size_number",            "type" : "number"}, DECIMAL
                                 {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                 {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                 {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                 {"name" : '"size_number_integer"',  "type" : "integer"},
                                 {"name" : '"size_number_int"',      "type" : "int"},
                                 {"name" : '"size_number_smallint"', "type" : "smallint"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = discover_catalog(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'size_number_10_-1':    {'maximum': 9999999999, 'minimum': -9999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_*_0':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_integer':  {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_4_0':      {'maximum': 9999, 'minimum': -9999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_int':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_smallint': {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'database_name': 'ROOT',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'is_view': False,
                              'row_count': 0,
                              'metadata': [{'metadata': {'key_properties': []}, 'breadcrumb': ()},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_*_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_10_-1')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_4_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_int')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_integer')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_smallint')}]},
                             stream_dict)


if __name__== "__main__":
    test1 = TestIntegerTableNoPK()
    test1.setUp()
    test1.test_catalog()
