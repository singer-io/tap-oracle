import simplejson as json
import sys
from singer import RecordMessage


def format_message(message,ensure_ascii=True):
    return json.dumps(message.asdict(), use_decimal=True, ensure_ascii=ensure_ascii)


def write_message(message, ensure_ascii=True):
    sys.stdout.write(format_message(message, ensure_ascii=ensure_ascii) + '\n')
    sys.stdout.flush()
