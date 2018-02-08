from singer import get_logger, metadata
import cx_Oracle
import os

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

        print(sql)
        cur.execute(sql)


def select_all_of_stream(stream):
    selected_metadata = [{'metadata': {'selected': True}, 'breadcrumb': ()}]

    new_md = metadata.to_map(stream.metadata)


    old_md = new_md.get(())
    old_md.update({'selected': True})

    for col_name, col_schema in stream.schema.properties.items():
        old_md = new_md.get(('properties', col_name))
        old_md.update({'selected' : True})

    stream.metadatata = metadata.to_list(new_md)
    return stream
