#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from functools import lru_cache

from .query_submitter import QuerySubmitter
from .genie_table import *
from .constants import *


class MetadataProcessor:

    @staticmethod
    def list_tables(connection, table_name=None, table_category=None, table_type=None):
        """
        Returns the tables metadata
        :param connection: SalesforceCDPConnection object
        :param table_name: table name for which we want metadata
        :param table_category: table_category( Related, Engagement, Profile) for which we want tables metadata
        :param table_type: table_type (DataLakeObject or DataModelObject or CalculatedInsights) for which we want tables metadata
        :return: Metadata of requested tables
        """ # noqa: E501
        request_params = {}
        if table_name is not None:
            request_params['entityName'] = table_name
        if table_category is not None:
            request_params['entityCategory'] = table_category
        if table_type is not None:
            request_params['entityType'] = table_type

        tables_metadata_json = MetadataProcessor.__describe_table_result(connection, request_params)

        return MetadataProcessor.__convert_metadata_json_to_genie_table(tables_metadata_json)

    @staticmethod
    def __convert_metadata_json_to_genie_table(tables_metadata_json):
        # check if it has metadata key else return None
        genie_table_list = []
        table_metadata_value_list = tables_metadata_json['metadata']
        for table_metadata in table_metadata_value_list:
            genie_table = GenieTable()
            if GENIE_TABLE_DISPLAY_NAME in table_metadata.keys():
                genie_table.display_name = table_metadata[GENIE_TABLE_DISPLAY_NAME]

            if GENIE_TABLE_NAME in table_metadata.keys():
                genie_table.name = table_metadata[GENIE_TABLE_NAME]

            if GENIE_TABLE_CATEGORY in table_metadata.keys():
                genie_table.category = table_metadata[GENIE_TABLE_CATEGORY]

            if GENIE_TABLE_PRIMARY_KEYS in table_metadata.keys():
                genie_table_primary_keys = []
                for primary_key in table_metadata[GENIE_TABLE_PRIMARY_KEYS]:
                    genie_table_primary_key = PrimaryKeys(
                        primary_key[PRIMARY_KEY_NAME],
                        primary_key[PRIMARY_KEY_DISPLAY_NAME],
                        primary_key[PRIMARY_KEY_INDEX_ORDER])
                    genie_table_primary_keys.append(genie_table_primary_key)
                genie_table.primary_keys = genie_table_primary_keys

            if GENIE_TABLE_PARTITION_BY in table_metadata.keys():
                genie_table.partition_by = table_metadata[GENIE_TABLE_PARTITION_BY]

            if GENIE_TABLE_RELATIONSHIPS in table_metadata.keys() and len(
                    table_metadata[GENIE_TABLE_RELATIONSHIPS]) > 0:
                genie_table_relationships = []
                for relationship in table_metadata[GENIE_TABLE_RELATIONSHIPS]:
                    genie_table_relationship = Relationship(
                        relationship[RELATIONSHIP_FROM_TABLE],
                        relationship[RELATIONSHIP_TO_TABLE])
                    if RELATIONSHIP_FROM_ENTITY_ATTRIBUTE in relationship.keys():
                        genie_table_relationship.from_entity_attribute = relationship[
                            RELATIONSHIP_FROM_ENTITY_ATTRIBUTE]
                    if RELATIONSHIP_TO_ENTITY_ATTRIBUTE in relationship.keys():
                        genie_table_relationship.to_entity_attribute = relationship[RELATIONSHIP_TO_ENTITY_ATTRIBUTE]
                    if RELATIONSHIP_CARDINALITY in relationship.keys():
                        genie_table_relationship.cardinality = relationship[RELATIONSHIP_CARDINALITY]

                    genie_table_relationships.append(genie_table_relationship)
                genie_table.relationships = genie_table_relationships

            if GENIE_TABLE_INDEXES in table_metadata.keys():
                if genie_table.indexes is None:
                    genie_table.indexes = []
                for index in table_metadata[GENIE_TABLE_INDEXES]:
                    fields = []
                    for json_field_obj in index[GENIE_TABLE_FIELDS]:
                        field = Field(name=json_field_obj[FIELDS_NAME], type=json_field_obj[FIELDS_TYPE])
                        fields.append(field)
                    genie_table.indexes.append(Index(fields))

            fields = MetadataProcessor.__get_fields_of_genie_table(table_metadata)
            genie_table.fields = fields
            genie_table_list.append(genie_table)

        return genie_table_list

    @staticmethod
    def __get_fields_of_genie_table(table_metadata):
        genie_table_fields = []
        if GENIE_TABLE_FIELDS in table_metadata.keys():
            for field in table_metadata[GENIE_TABLE_FIELDS]:
                genie_table_field = Field(field[FIELDS_NAME], field[FIELDS_DISPLAY_NAME], field[FIELDS_TYPE])
                genie_table_fields.append(genie_table_field)
        else:
            if GENIE_TABLE_DIMENSIONS in table_metadata.keys():
                for dimension in table_metadata[GENIE_TABLE_DIMENSIONS]:
                    genie_table_field_from_dimension = Field(dimension[FIELDS_NAME],
                                                             dimension[FIELDS_DISPLAY_NAME],
                                                             dimension[FIELDS_TYPE],
                                                             is_measure=False, is_dimension=True)
                    genie_table_fields.append(genie_table_field_from_dimension)

            if GENIE_TABLE_MEASURES in table_metadata.keys():
                for measure in table_metadata[GENIE_TABLE_MEASURES]:
                    genie_table_field_from_measure = Field(measure[FIELDS_NAME],
                                                           measure[FIELDS_DISPLAY_NAME],
                                                           measure[FIELDS_TYPE],
                                                           is_measure=True, is_dimension=False)
                    genie_table_fields.append(genie_table_field_from_measure)
        return genie_table_fields

    @staticmethod
    def __describe_table_result(connection, request_params={}):
        result = QuerySubmitter.get_metadata(connection, request_params)
        return result
