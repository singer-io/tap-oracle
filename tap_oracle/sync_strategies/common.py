import singer
import decimal
import datetime
import dateutil.parser

DEFAULT_TZINFO = datetime.tzinfo(0)
# SELECT TZ_OFFSET(DBTIMEZONE) FROM DUAL;
def row_to_singer_message(stream, row, version, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = stream.schema.properties[columns[idx]].type
        multiple_of = stream.schema.properties[columns[idx]].multipleOf
        format = stream.schema.properties[columns[idx]].format #date-time
        if elem is None:
            row_to_persist += (elem,)
        elif 'integer' in property_type or property_type == 'integer':
            integer_representation = int(elem)
            row_to_persist += (integer_representation,)
        elif ('number' in property_type or property_type == 'number') and multiple_of:
            decimal_representation = decimal.Decimal(elem)
            row_to_persist += (decimal_representation,)
        elif ('number' in property_type or property_type == 'number'):
            row_to_persist += (float(elem),)
        elif format == 'date-time':
            row_to_persist += (elem,)
        else:
            row_to_persist += (elem,)

    rec = dict(zip(columns, row_to_persist))
    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)
