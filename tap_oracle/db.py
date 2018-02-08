
def quote_identifier(identifier):
   return identifier.replace('"', r'\"')

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(quote_identifier(schema), quote_identifier(table), quote_identifier(column))
