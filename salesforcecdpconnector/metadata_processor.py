#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from .query_submitter import QuerySubmitter


class MetadataProcessor:

    @staticmethod
    def describe_table(connection, entity_name=None, entity_category=None, entity_type=None):
        """
        Returns the tables metadata for a given tenant or given tables or given data table type
        :param connection: SalesforceCDPConnection object
        :param entity_name: table name for which we want metadata
        :param entity_category: entityCategory( Related, Engagement, Profile) for which we want tables metadata
        :param entity_type: entityType for (dll, dlm, dlo) which we want tables metadata
        :return: Metadata of requested tables
        """
        request_params = {}
        if entity_name is not None and entity_name != '':
            request_params['entityName'] = entity_name
        if entity_category is not None and entity_category != '':
            request_params['entityCategory'] = entity_category
        if entity_type is not None and entity_type != '':
            request_params['entityType'] = entity_type

        tables_metadata_json = MetadataProcessor.__describe_table_result(connection, request_params)
        return tables_metadata_json

    @staticmethod
    def get_tables(connection):
        """
        Returns the list of table names for a given tenant
        :param connection: SalesforceCDPConnection object
        :return: List of tables for a given tenant
        """
        tables_metadata_json = MetadataProcessor.__describe_table_result(connection)
        table_list = []
        tables_metadata = tables_metadata_json['metadata']
        for table_metadata in tables_metadata:
            if 'displayName' in table_metadata.keys():
                table_list.append(table_metadata["displayName"])
        return table_list

    @staticmethod
    def __describe_table_result(connection, request_params={}):
        result = QuerySubmitter.get_metadata(connection, request_params)
        return result
