#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import base64

import dateutil.parser
import pyarrow

from .constants import API_VERSION_V2
from .constants import QUERY_RESPONSE_KEY_DONE
from .constants import QUERY_RESPONSE_KEY_NEXT_BATCH_ID
from .constants import QUERY_RESPONSE_KEY_ARROW_STREAM
from .constants import QUERY_RESPONSE_KEY_METADATA
from .constants import QUERY_RESPONSE_KEY_METADATA_TYPE
from .constants import DATA_TYPE_TIMESTAMP
from .constants import ENCODING_ASCII
from .query_submitter import QuerySubmitter


class PandasUtils:

    @staticmethod
    def get_dataframe(connection, query):
        """
        Executes the query and returns results as a Pandas dataframe
        :param connection: SalesforceCDPConnection object
        :param query: The query to be executed
        :return: Query results as Pandas Dataframe
        """
        arrow_stream_list = []
        result = QuerySubmitter.execute(connection, query, API_VERSION_V2, True)
        encoded_arrow_stream = result[QUERY_RESPONSE_KEY_ARROW_STREAM]
        arrow_table = PandasUtils._get_pyarrow_table(encoded_arrow_stream)
        PandasUtils._add_table_to_list(arrow_stream_list, arrow_table)
        while result[QUERY_RESPONSE_KEY_DONE] is not True:
            result = QuerySubmitter.get_next_batch(connection, result[QUERY_RESPONSE_KEY_NEXT_BATCH_ID], True)
            encoded_arrow_stream = result[QUERY_RESPONSE_KEY_ARROW_STREAM]
            arrow_table = PandasUtils._get_pyarrow_table(encoded_arrow_stream)
            PandasUtils._add_table_to_list(arrow_stream_list, arrow_table)

        if len(arrow_stream_list) > 0:
            pandas_df = pyarrow.concat_tables(arrow_stream_list).to_pandas()
            date_columns = PandasUtils._get_date_columns(result)
            for date_column in date_columns:
                pandas_df = pandas_df.apply(lambda row: PandasUtils._convert_to_date(row, date_column), axis=1)
            return pandas_df

        return None

    @staticmethod
    def _convert_to_date(row, column):
        value = row[column]
        if isinstance(value, str):
            row[column] = dateutil.parser.parse(value)
        else:
            row[column] = None
        return row

    @staticmethod
    def _get_date_columns(result):
        metadata = result[QUERY_RESPONSE_KEY_METADATA]
        date_columns = [x for x in metadata.keys() if PandasUtils._istimestamp(x, metadata)]
        return date_columns

    @staticmethod
    def _istimestamp(key, metadata_list):
        metadata_type = metadata_list[key][QUERY_RESPONSE_KEY_METADATA_TYPE]
        if metadata_type is not None:
            metadata_type = metadata_type.upper()
        return metadata_type == DATA_TYPE_TIMESTAMP

    @staticmethod
    def _get_pyarrow_table(encoded_arrow_stream):
        if encoded_arrow_stream is None:
            return None
        stream_bytes = encoded_arrow_stream.encode(ENCODING_ASCII)
        decoded_bytes = base64.b64decode(stream_bytes)
        return pyarrow.ipc.open_stream(decoded_bytes).read_all()

    @staticmethod
    def _add_table_to_list(arrow_stream_list, arrow_stream):
        if arrow_stream is not None:
            arrow_stream_list.append(arrow_stream)
