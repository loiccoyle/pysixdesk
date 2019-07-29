import collections


def bigint_check(val):
    if not isinstance(val, collections.Iterable) or isinstance(val, str):
        val = [val]
    return any([v > 2147483647 for v in val])


class SQLiteDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['NoneType'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'FLOAT'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'INT'

    def __getitem__(self, param):
        '''Get the corresponding sqlite data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        sql_type = self.db_type[a.__name__]
        # not pretty...
        if sql_type == 'INT' and bigint_check(param):
            return 'BIGINT'
        else:
            return sql_type


class MySQLDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['NoneType'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'DOUBLE'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'TEXT'

    def __getitem__(self, param):
        '''Get the corresponding mysql data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        msql_type = self.db_type[a.__name__]
        # not pretty...
        if msql_type == 'INT' and bigint_check(param):
            return 'BIGINT'
        else:
            return msql_type
