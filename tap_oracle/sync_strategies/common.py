import singer

def row_to_singer_message(stream, row, version, columns, time_extracted):
    rec = dict(zip(columns, row))
    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)
