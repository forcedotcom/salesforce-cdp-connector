#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from .query_submitter import QuerySubmitter
from .constants import API_VERSION_V1


class MetadataProcessor:

    @staticmethod
    def describe_table(connection, entity_name='', entity_category='', entity_type=''):
        """
        Returns the tables metadata for a given tenant or given tables or given data table type
        :param connection: SalesforceCDPConnection object
        :param entity_name: table name for which we want metadata
        :param entity_category: entityCategory( Related, Engagement, Profile) for which we want tables metadata
        :param entity_type: entityType for (dll, dlm, dlo) which we want tables metadata
        :return: Metadata of requested tables
        """
        tables_metadata_json = MetadataProcessor.__describe_table_result(connection)
        if entity_name != "":
            # get results of just that one table name
            tables_metadata_json = MetadataProcessor.__describe_table_by_entity_name(tables_metadata_json,
                                                                                     entity_name)
        elif entity_category != "":
            # get results of a particular category whether Profile, Engagement or Related
            tables_metadata_json = MetadataProcessor.__describe_table_by_entity_category(tables_metadata_json,
                                                                                         entity_category)
        elif entity_type != "":
            # get results of a particular type whether dll, dlm or dlo
            tables_metadata_json = MetadataProcessor.__describe_table_by_entity_type(tables_metadata_json,
                                                                                     entity_type)

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
    def __describe_table_result(connection):
        result = QuerySubmitter.get_metadata(connection, API_VERSION_V1, False)
        return result

    @staticmethod
    def __describe_table_by_entity_name(tables_metadata_json, entity_name):
        tables_metadata = tables_metadata_json['metadata']
        for table_metadata in tables_metadata:
            if 'displayName' in table_metadata.keys() and table_metadata["displayName"] == entity_name:
                return table_metadata
        return None

    @staticmethod
    def __describe_table_by_entity_category(tables_metadata_json, entity_category):
        table_metadata_by_entity_category = []
        tables_metadata = tables_metadata_json['metadata']
        for table_metadata in tables_metadata:
            if 'category' in table_metadata.keys() and table_metadata["category"].lower() == entity_category.lower():
                table_metadata_by_entity_category.append(table_metadata)
        return table_metadata_by_entity_category

    @staticmethod
    def __describe_table_by_entity_type(tables_metadata_json, entity_type):
        table_metadata_by_entity_type = []
        tables_metadata = tables_metadata_json['metadata']
        for table_metadata in tables_metadata:
            # checking the category based on the last three characters of the name
            if 'name' in table_metadata.keys() and table_metadata["name"][-3:] == entity_type.lower():
                table_metadata_by_entity_type.append(table_metadata)
        return table_metadata_by_entity_type
