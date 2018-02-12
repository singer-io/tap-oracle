import singer

def row_to_singer_message(stream, row, version, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = stream.schema.properties[columns[idx]].type
        if elem is None:
            row_to_persist += (elem,)
        elif 'integer' in property_type or property_type == 'integer':
            integer_representation = int(elem)
            row_to_persist += (integer_representation,)
        else:
            row_to_persist += (elem,)

    rec = dict(zip(columns, row_to_persist))
    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)
