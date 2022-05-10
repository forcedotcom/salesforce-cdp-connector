#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
import dateutil.parser

from .constants import QUERY_RESPONSE_KEY_DATA
from .constants import QUERY_RESPONSE_KEY_METADATA
from .constants import QUERY_RESPONSE_KEY_DONE
from .constants import QUERY_RESPONSE_KEY_NEXT_BATCH_ID
from .constants import DATA_TYPE_TIMESTAMP
from .constants import QUERY_RESPONSE_KEY_PLACE_IN_ORDER
from .parsed_query_result import QueryResult


class QueryResultParser:

    @staticmethod
    def parse_result(result):
        """
        Parses the json response from queryV2 API
        :param result: JSON response from queryV2 API
        :return: ParsedQueryResult
        """
        return QueryResultParser._parse_v2_result(result)

    @staticmethod
    def _parse_v2_result(result):
        data = result[QUERY_RESPONSE_KEY_DATA]
        metadata_dict = result[QUERY_RESPONSE_KEY_METADATA]
        is_done = result[QUERY_RESPONSE_KEY_DONE]
        next_batch_id = result.get(QUERY_RESPONSE_KEY_NEXT_BATCH_ID)
        has_next = not is_done
        sorted_metadata_items = QueryResultParser._sort_metadata_by_place_in_order(metadata_dict)
        description = QueryResultParser._convert_metadata_list_to_description(sorted_metadata_items)
        QueryResultParser._convert_timestamps(data, description)
        return QueryResult(data, description, has_next, next_batch_id)

    @staticmethod
    def _convert_timestamps(data, description):
        """
        TIMESTAMPS are coming as string in JSON. This function will update the string to datetime object
        :param data: List of JSON results
        :param description: Cursor description
        :return: None
        """
        for i in range(0, len(description)):
            if description[i][1] == DATA_TYPE_TIMESTAMP:
                for data_row in data:
                    if data_row[i] is not None and isinstance(data_row[i], str) and len(data_row[i]) > 0:
                        data_row[i] = dateutil.parser.parse(data_row[i])

    @staticmethod
    def _convert_metadata_item_to_description_item(metadata_item):
        metadata_name = metadata_item[0]
        type = metadata_item[1]['type']
        return QueryResultParser._get_description_item(metadata_name, type)

    @staticmethod
    def _get_description_item(name, data_type):
        """
        This will generate the description to be used with Cursor
        :param name: Column Name
        :param data_type: Column Type
        :return: One tuple representing description for a column
        """
        return (
            name,  # Column Name
            data_type,  # Column Type
            None,
            None,
            None,
            None,
            None
        )

    @staticmethod
    def _convert_metadata_list_to_description(sorted_metadata_list):
        return [QueryResultParser._convert_metadata_item_to_description_item(metadata_item) for metadata_item in
                sorted_metadata_list]

    @staticmethod
    def _sort_metadata_by_place_in_order(metadata_dict):
        """
        This functions sorts the column metadata from queryV2 response based on the placeInOrder field.
        :param metadata_dict: The metadata dict from JSON
        :return: sorted column metadata as List
        """
        metadataList = [metadataItem for metadataItem in metadata_dict.items()]
        metadataList = sorted(metadataList, key=lambda item: item[1][QUERY_RESPONSE_KEY_PLACE_IN_ORDER])
        return metadataList
