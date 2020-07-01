import unittest
import cx_Oracle, sys, string, datetime
import tap_oracle
import os
import pdb
from singer import get_logger
import tap_oracle.sync_strategies.full_table as full_table
try:
    from tests.utils import get_test_connection, get_test_conn_config, ensure_test_table
except ImportError:
    from utils import get_test_connection, ensure_test_table

LOGGER = get_logger()

def do_not_dump_catalog(catalog):
    pass


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
                                 {"name" : '"name-varchar2-explicit-char"',  "type": "varchar2(251 char)"}],
                      "name" : "CHICKEN"}
       ensure_test_table(table_spec)
       tap_oracle.dump_catalog = do_not_dump_catalog
       full_table.UPDATE_BOOKMARK_PERIOD = 1000


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            self.assertEqual('CHICKEN', stream_dict.get('table_name'))
            self.assertEqual('CHICKEN', stream_dict.get('stream'))
            self.assertEqual('ROOT-CHICKEN', stream_dict.get('tap_stream_id'))

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(stream_dict.get('metadata'),
                             [{'metadata': {'table-key-properties': ['ID'],
                                            'database-name': os.getenv('TAP_ORACLE_SID'),
                                            'schema-name': 'ROOT',
                                            'is-view': False,
                                            'row-count': 0},
                               'breadcrumb': ()},
                              {'metadata': {'inclusion': 'automatic', 'sql-datatype' : 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'ID'),},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'CHAR', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-char-explicit-byte')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'CHAR', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-char-explicit-char')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'NCHAR', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-nchar')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'NVARCHAR2', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-nvarchar2')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'VARCHAR2', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-varchar-explicit-byte')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'VARCHAR2', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-varchar-explicit-char')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'VARCHAR2', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-varchar2-explicit-byte')},
                              {'metadata': {'inclusion': 'available', 'sql-datatype' : 'VARCHAR2', 'selected-by-default': True}, 'breadcrumb': ('properties', 'name-varchar2-explicit-char')}])

            self.assertEqual({'properties': {'ID':                      {'type': ['integer']},
                                             'name-char-explicit-byte': {'type': ['null', 'string']},
                                             'name-char-explicit-char': {'type': ['null', 'string'], 'maxLength': 250},

                                             'name-nchar':     {'type': ['null', 'string'], 'maxLength': 123 },
                                             'name-nvarchar2': {'type': ['null', 'string'], 'maxLength': 234 },

                                             'name-varchar-explicit-byte': {'type': ['null', 'string']},
                                             'name-varchar-explicit-char': {'type': ['null', 'string'], 'maxLength': 251},

                                             'name-varchar2-explicit-byte': {'type': ['null', 'string']},
                                             'name-varchar2-explicit-char': {'type': ['null', 'string'], 'maxLength': 251}},
                              'type': 'object'},  stream_dict.get('schema'))


class TestIntegerTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" :  "size_pk   ",            "type" : "number(4,0)", "primary_key" : True, "identity" : True},
                                 {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                 {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                 {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                 {"name" : '"size_number_integer"',  "type" : "integer"},
                                 {"name" : '"size_number_int"',      "type" : "int"},
                                 {"name" : '"size_number_4"',        "type" : "number(4)"},
                                 {"name" : '"size_number_smallint"', "type" : "smallint"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)
       tap_oracle.dump_catalog = do_not_dump_catalog
       full_table.UPDATE_BOOKMARK_PERIOD = 1000


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'size_number_10_-1':    {'type': ['null', 'integer']},
                                                        'size_number_*_0':      {'type': ['null', 'integer']},
                                                        'size_number_integer':  {'type': ['null', 'integer']},
                                                        'size_number_4':      {'type': ['null', 'integer']},
                                                        'size_number_4_0':      {'type': ['null', 'integer']},
                                                        'size_number_int':      {'type': ['null', 'integer']},
                                                        'size_number_smallint': {'type': ['null', 'integer']},
                                                        'SIZE_PK':              {'type': ['integer']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'metadata': {'table-key-properties': ['SIZE_PK'],
                                                         'database-name': os.getenv('TAP_ORACLE_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0},
                                            'breadcrumb': ()},
                                           {'metadata': {'inclusion': 'automatic', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'SIZE_PK')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_*_0')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_10_-1')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_4')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_4_0')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_int')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_integer')},
                                           {'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}, 'breadcrumb': ('properties', 'size_number_smallint')}]},

                             stream_dict)



class TestDecimalPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_number"',                "type" : "number", "primary_key": True},
                                 {"name" : '"our_number_10_2"',           "type" : "number(10,2)"},
                                 {"name" : '"our_number_38_4"',           "type" : "number(38,4)"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)
       tap_oracle.dump_catalog = do_not_dump_catalog
       full_table.UPDATE_BOOKMARK_PERIOD = 1000


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()
            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_number': {'format': 'singer.decimal',
                                                                       'type': ['string']},
                                                        'our_number_10_2': {'format': 'singer.decimal',
                                                                            'type': ['null', 'string']},
                                                        'our_number_38_4': {'format': 'singer.decimal',
                                                                            'type': ['null', 'string']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'breadcrumb': (),
                                            'metadata': {'table-key-properties': ['our_number'],
                                                         'database-name': os.getenv('TAP_ORACLE_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0}},
                                           {'breadcrumb': ('properties', 'our_number'),      'metadata': {'inclusion': 'automatic', 'sql-datatype': 'NUMBER', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_number_10_2'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_number_38_4'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'NUMBER', 'selected-by-default': True}}]},
                             stream_dict)


class TestDatesTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_date"',                   "type" : "DATE", "primary_key": True },
                                 {"name" : '"our_ts"',                     "type" : "TIMESTAMP"},
                                 {"name" : '"our_ts_tz"',                  "type" : "TIMESTAMP WITH TIME ZONE"},
                                 {"name" : '"our_ts_tz_local"',            "type" : "TIMESTAMP WITH LOCAL TIME ZONE"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)
       tap_oracle.dump_catalog = do_not_dump_catalog
       full_table.UPDATE_BOOKMARK_PERIOD = 1000


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'our_date':               {'type': ['string'], 'format' : 'date-time'},
                                                        'our_ts':                 {'type': ['null', 'string'], 'format' : 'date-time'},
                                                        'our_ts_tz':              {'type': ['null', 'string'], 'format' : 'date-time'},
                                                        'our_ts_tz_local':        {'type': ['null', 'string'], 'format' : 'date-time'}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata':
                              [{'breadcrumb': (),
                                'metadata': {'table-key-properties': ['our_date'],
                                             'database-name': os.getenv('TAP_ORACLE_SID'),
                                             'schema-name': 'ROOT',
                                             'is-view': 0,
                                             'row-count': 0}},
                               {'breadcrumb': ('properties', 'our_date'),        'metadata': {'inclusion': 'automatic', 'sql-datatype': 'DATE', 'selected-by-default': True}},
                               {'breadcrumb': ('properties', 'our_ts'),          'metadata': {'inclusion': 'available', 'sql-datatype': 'TIMESTAMP(6)', 'selected-by-default': True}},
                               {'breadcrumb': ('properties', 'our_ts_tz'),       'metadata': {'inclusion': 'available', 'sql-datatype': 'TIMESTAMP(6) WITH TIME ZONE', 'selected-by-default': True}},
                               {'breadcrumb': ('properties', 'our_ts_tz_local'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'TIMESTAMP(6) WITH LOCAL TIME ZONE', 'selected-by-default': True}}]},

                             stream_dict)


class TestFloatTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_float"',                 "type" : "float", "primary_key": True },
                                 {"name" : '"our_double_precision"',      "type" : "double precision"},
                                 {"name" : '"our_real"',                  "type" : "real"},
                                 {"name" : '"our_binary_float"',          "type" : "binary_float"},
                                 {"name" : '"our_binary_double"',         "type" : "binary_double"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)
       tap_oracle.dump_catalog = do_not_dump_catalog
       full_table.UPDATE_BOOKMARK_PERIOD = 1000

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), [])
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_float':               {'type': ['string'],
                                                                                    'format': 'singer.decimal'},
                                                        'our_double_precision':    {'type': ['null', 'string'],
                                                                                    'format': 'singer.decimal'},
                                                        'our_real':                {'type': ['null', 'string'],
                                                                                    'format': 'singer.decimal'},
                                                        'our_binary_float':        {'type': ['null', 'number']},
                                                        'our_binary_double':       {'type': ['null', 'number']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'breadcrumb': (),
                                            'metadata': {'table-key-properties': ["our_float"],
                                                         'database-name': os.getenv('TAP_ORACLE_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0}},
                                           {'breadcrumb': ('properties', 'our_binary_double'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'BINARY_DOUBLE', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_binary_float'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'BINARY_FLOAT', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_double_precision'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'FLOAT', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_float'), 'metadata': {'inclusion': 'automatic', 'sql-datatype': 'FLOAT', 'selected-by-default': True}},
                                           {'breadcrumb': ('properties', 'our_real'), 'metadata': {'inclusion': 'available', 'sql-datatype': 'FLOAT', 'selected-by-default': True}}]},
                             stream_dict)


class TestFilterSchemas(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        table_spec = {"columns": [{"name" :  "size_pk   ",            "type" : "number(4,0)", "primary_key" : True, "identity" : True},
                                  {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                  {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                  {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                  {"name" : '"size_number_integer"',  "type" : "integer"},
                                  {"name" : '"size_number_int"',      "type" : "int"},
                                  {"name" : '"size_number_smallint"', "type" : "smallint"}],
                      "name" : "CHICKEN"}
        ensure_test_table(table_spec)
        tap_oracle.dump_catalog = do_not_dump_catalog
        full_table.UPDATE_BOOKMARK_PERIOD = 1000


    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(get_test_conn_config(), ['ROOT'])

            discovered_streams = {}
            for s in catalog.streams:
                schema_name = [md['metadata']['schema-name'] for md in s.metadata if md['breadcrumb'] == () and md['metadata']][0]
                if discovered_streams.get(schema_name) is None:
                    discovered_streams[schema_name] = [s.tap_stream_id]
                else:
                    discovered_streams[schema_name].append(s.tap_stream_id)

            self.assertEqual(list(discovered_streams.keys()),  ['ROOT'])

if __name__== "__main__":
    test1 = TestFilterSchemas()
    test1.setUp()
    test1.test_catalog()
