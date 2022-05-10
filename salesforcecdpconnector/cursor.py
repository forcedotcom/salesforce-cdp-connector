#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from datetime import date, time, datetime

from .exceptions import NotSupportedError, Error
from .query_result_parser import QueryResultParser
from .query_submitter import QuerySubmitter


class SalesforceCDPCursor:
    """
    This class represents the cursor
    """

    _TRANSLATION_TABLE = str.maketrans({"\\": r"\\",
                                        "\n": r"\\n",
                                        "\r": r"\\r",
                                        "'": r"\'"})

    def __init__(self, connection):
        self.arraysize = 1
        self.description = None
        self.data = None
        self.connection = connection
        self.current_query = None
        self.has_next = None
        self.next_batch_id = None
        self.closed = False
        self.has_result = False

    def execute(self, query, params=None):
        """
        Executes the query and loads the first batch of results. Supports only read only queries
        :param query: The input query.
        :param params: The parameters to the query
        :return: None
        """
        self._check_cursor_closed()
        self.current_query = self._resolve_query_with_params(query, params)
        json_results = QuerySubmitter.execute(self.connection, self.current_query)
        results = QueryResultParser.parse_result(json_results)
        self.description = results.description
        self.data = results.data
        self.has_next = results.has_next
        self.next_batch_id = results.next_batch_id
        self.has_result = True

    def fetchall(self):
        """
        To be called after the execute function. This will return the entire results of the query executed.
        :return: Returns the query results
        """
        if not self.has_result:
            raise Error('No results available to fetch')
        while self.has_next is True:
            self._check_cursor_closed()
            json_results = QuerySubmitter.get_next_batch(self.connection, self.next_batch_id)
            results = QueryResultParser.parse_result(json_results)
            self.description = results.description
            self.has_next = results.has_next
            self.next_batch_id = results.next_batch_id
            self.data = self.data + results.data
        self._check_cursor_closed()
        self.has_result = False
        return self.data

    def fetchone(self):
        """
        To be called after the execute function. This will return next row from the query results.
        :return: Returns the next row from query result. Returns None if no more data is present.
        """
        if not self.has_result:
            raise Error('No results available to fetch')
        self._check_cursor_closed()
        if self.data is not None and len(self.data) > 0:
            next_row = self.data.pop(0)
            return next_row
        elif self.has_next is True:
            json_results = QuerySubmitter.get_next_batch(self.connection, self.next_batch_id)
            results = QueryResultParser.parse_result(json_results)
            self.description = results.description
            self.data = results.data
            self.has_next = results.has_next
            self.next_batch_id = results.next_batch_id
            next_row = self.data.pop(0)
            return next_row
        else:
            self.has_result = False
            return None

    def fetchmany(self, size=None):
        """
        Returns size number of rows from result
        :param size: If not provided cursor.arraysize will be used by default
        :return: size number of rows from result
        """
        if size is None:
            size = self.arraysize
        result = []
        while size > 0:
            next_row = self.fetchone()
            size = size - 1
            if next_row is None:
                break
            result.append(next_row)
        return result

    def close(self):
        """
        Nothing to close
        :return: None
        """
        self.closed = True

    def rollback(self):
        """
        Nothing to rollback
        :return: None
        """
        pass

    def commit(self):
        """
        Nothing to commit
        :return: None
        """
        pass

    def setinputsizes(self):
        """
        Do nothing
        :return:
        """
        pass

    def setoutputsize(self):
        """
        Do nothing
        :return:
        """
        pass

    def _check_cursor_closed(self):
        if self.closed:
            raise Error('Attempting operation while cursor is closed')
        elif self.connection.closed:
            raise Error('Attempting operation while connection is closed')

    def _resolve_query_with_params(self, query, params):
        params_count_from_query = query.count('?')
        if params is None and params_count_from_query == 0:
            return query

        if self._is_iterable(params) and params_count_from_query == len(params):
            for param in params:
                query = self._replace_next_param(query, param)
        elif params_count_from_query == 1 and params is not None:
            query = self._replace_next_param(query, params)
        else:
            raise Exception('Parameter count not matching')

        return query

    def _replace_next_param(self, query, param):
        if param is None:
            query = query.replace('?', 'null', 1)
        elif self._is_numeric(param):
            query = query.replace('?', str(param), 1)
        else:
            if not isinstance(param, str):
                param = str(param)
            param = param.translate(self._TRANSLATION_TABLE)
            query = query.replace('?', f"'{str(param)}'", 1)
        return query

    def _is_iterable(self, param):
        try:
            iter(param)
            return True
        except Exception:
            pass
        return False

    def _is_numeric(self, param):
        return isinstance(param, int) or \
            isinstance(param, float)

    def executemany(self, **kwargs):
        raise NotSupportedError('executemany is not supported')


def Date(year, month, day):
    """Construct an object holding a date value."""
    return date(year, month, day)


def Time(hour, minute=0, second=0, microsecond=0, tzinfo=None):
    """Construct an object holding a time value."""
    return time(hour, minute, second, microsecond, tzinfo)


def Timestamp(year, month, day, hour=0, minute=0, second=0, microsecond=0,
              tzinfo=None):
    """Construct an object holding a time stamp value."""
    return datetime(year, month, day, hour, minute, second, microsecond,
                    tzinfo)


def DateFromTicks(ticks):
    return date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return datetime(*time.localtime(ticks)[:6])


class Binary(bytes):
    """Construct an object capable of holding a binary (long) string value."""


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = [v.lower() for v in values]

    def __eq__(self, other):
        return other.lower() in self.values


STRING = DBAPITypeObject("VARCHAR", "CHAR", "VARBINARY", "JSON")

NUMBER = DBAPITypeObject(
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DOUBLE", "DECIMAL"
)

DATETIME = DBAPITypeObject(
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP"
)
