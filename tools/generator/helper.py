# coding=utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def change_file_type(fild_type):
    fild_type_lower = fild_type.lower()
    if 'int' in fild_type_lower:
        return 'bigint'
    elif 'char' in fild_type_lower or 'text' in fild_type_lower:
        return 'string'
    elif 'date' in fild_type_lower or 'time' in fild_type_lower:
        return 'string'
    elif 'double' in fild_type_lower or 'decimal' in fild_type_lower:
        return 'double'
    else:
        return "string"
