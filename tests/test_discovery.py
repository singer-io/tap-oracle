import unittest
import cx_Oracle, sys, string, _thread, datetime
import tap_oracle
import pdb
from singer import get_logger
from tests.utils import get_test_connection, ensure_test_table

LOGGER = get_logger()

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

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(conn)
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


class TestIntegerTablePK(unittest.TestCase):
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

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'size_number_10_-1':    {'maximum': 9999999999, 'minimum': -9999999999,
                                                                                 'type': ['null', 'integer'],
                                                                                 'multipleOf': 10 },
                                                        'size_number_*_0':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_integer':  {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_4_0':      {'maximum': 9999, 'minimum': -9999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_int':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_smallint': {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'SIZE_PK':               {'maximum': 9999, 'minimum': -9999,
                                                                                  'type': ['integer']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'database_name': 'ROOT',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'is_view': False,
                              'row_count': 0,
                              'metadata': [{'metadata': {'key_properties': ['SIZE_PK']}, 'breadcrumb': ()},
                                           {'metadata': {'inclusion': 'automatic'}, 'breadcrumb': ('properties', 'SIZE_PK')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_*_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_10_-1')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_4_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_int')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_integer')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_smallint')}]},

                             stream_dict)



class TestDecimalPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_number"',                "type" : "number", "primary_key": True},
                                 {"name" : '"our_number_10_2"',           "type" : "number(10,2)"},
                                 {"name" : '"our_number_38_4"',           "type" : "number(38,4)"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()
            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_number': {'maximum': 99999999999999999999999999999999999999,
                                                                       'minimum': -99999999999999999999999999999999999999,
                                                                       'type': [ 'integer']},
                                                        'our_number_10_2': {'exclusiveMaximum': True,
                                                                            'exclusiveMinimum': True,
                                                                            'maximum': 100000000,
                                                                            'minimum': -100000000,
                                                                            'multipleOf': 0.01,
                                                                            'type': ['null', 'number']},
                                                        'our_number_38_4': {'exclusiveMaximum': True,
                                                                             'exclusiveMinimum': True,
                                                                             'maximum': 10000000000000000000000000000000000,
                                                                             'minimum': -10000000000000000000000000000000000,
                                                                             'multipleOf': 0.0001,
                                                                             'type': ['null', 'number']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'database_name': 'ROOT',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'is_view': False,
                              'row_count': 0,
                              'metadata': [{'breadcrumb': (), 'metadata': {'key_properties': ['our_number']}},
                                           {'breadcrumb': ('properties', 'our_number'), 'metadata': {'inclusion': 'automatic'}},
                                           {'breadcrumb': ('properties', 'our_number_10_2'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_number_38_4'), 'metadata': {'inclusion': 'available'}}]},
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

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(conn)
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
                              'database_name': 'ROOT',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'is_view': False,
                              'row_count': 0,
                              'metadata':
                              [{'breadcrumb': (), 'metadata': {'key_properties': ['our_date']}},
                               {'breadcrumb': ('properties', 'our_date'),
                                'metadata': {'inclusion': 'automatic'}},
                               {'breadcrumb': ('properties', 'our_ts'),
                                'metadata': {'inclusion': 'available'}},
                               {'breadcrumb': ('properties', 'our_ts_tz'),
                                'metadata': {'inclusion': 'available'}},
                               {'breadcrumb': ('properties', 'our_ts_tz_local'),
                                'metadata': {'inclusion': 'available'}}]},

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

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_oracle.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_float':               {'type': ['number']},
                                                        'our_double_precision':    {'type': ['null', 'number']},
                                                        'our_real':                {'type': ['null', 'number']},
                                                        'our_binary_float':        {'type': ['null', 'number']},
                                                        'our_binary_double':       {'type': ['null', 'number']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'database_name': 'ROOT',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'is_view': False,
                              'row_count': 0,
                              'metadata': [{'breadcrumb': (), 'metadata': {'key_properties': ["our_float"]}},
                                           {'breadcrumb': ('properties', 'our_binary_double'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_binary_float'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_double_precision'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_float'), 'metadata': {'inclusion': 'automatic'}},
                                           {'breadcrumb': ('properties', 'our_real'), 'metadata': {'inclusion': 'available'}}]},
                             stream_dict)
if __name__== "__main__":
    test1 = TestDatesTablePK()
    test1.setUp()
    test1.test_catalog()
