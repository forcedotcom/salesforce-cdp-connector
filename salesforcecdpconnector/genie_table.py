#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

class PrimaryKeys:

    def __init__(self, name=None, display_name=None, index_order=None):
        self.name = name
        self.display_name = display_name
        self.index_order = index_order

    def __eq__(self, other):
        return self.name == other.name and self.display_name == other.display_name \
               and self.index_order == other.index_order

    def __repr__(self) -> str:
        return str(self.__dict__)

    def __str__(self) -> str:
        return str(self.__dict__)


class Relationship:

    def __init__(self, from_table=None, to_table=None, from_entity_attribute=None, to_entity_attribute=None,
                 cardinality=None):
        self.from_table = from_table
        self.to_table = to_table
        self.from_entity_attribute = from_entity_attribute
        self.to_entity_attribute = to_entity_attribute
        self.cardinality = cardinality

    def __eq__(self, other):
        return self.from_table == other.from_table and self.to_table == other.to_table \
               and self.from_entity_attribute == other.from_entity_attribute \
               and self.to_entity_attribute == other.to_entity_attribute and self.cardinality == other.cardinality

    def __repr__(self) -> str:
        return str(self.__dict__)

    def __str__(self) -> str:
        return str(self.__dict__)


class Field:

    def __init__(self, name=None, display_name=None, type=None, is_measure=False, is_dimension=False):
        self.name = name
        self.display_name = display_name
        self.type = type
        self.is_measure = is_measure
        self.is_dimension = is_dimension

    def __eq__(self, other):
        return self.name == other.name and self.display_name == other.display_name \
               and self.type == other.type and self.is_measure == other.is_measure \
               and self.is_dimension == other.is_dimension

    def __repr__(self) -> str:
        return str(self.__dict__)

    def __str__(self) -> str:
        return str(self.__dict__)


class Index:

    def __init__(self, fields=[]):
        self.fields = fields

    def __repr__(self) -> str:
        return str(self.__dict__)

    def __str__(self) -> str:
        return str(self.__dict__)


class GenieTable:

    def __init__(self, name=None, display_name=None, category=None, partition_by=None, primary_keys=[], fields=[],
                 relationships=[], indexes=[]):
        self.name = name
        self.display_name = display_name
        self.category = category
        self.primary_keys = primary_keys
        self.partition_by = partition_by
        self.fields = fields
        self.relationships = relationships
        self.indexes = indexes

    def __eq__(self, other):
        return self.name == other.name and self.display_name == other.display_name \
               and self.category == other.category and all(
            self.primary_keys[i] == other.primary_keys[i] for i in range(len(self.primary_keys))) \
               and self.partition_by == other.partition_by and all(
            self.fields[i] == other.fields[i] for i in range(len(self.fields))) \
               and all(self.relationships[i] == other.relationships[i] for i in range(len(self.relationships))) \
               and all(self.indexes[i] == other.indexes[i] for i in range(len(self.indexes)))

    def __repr__(self) -> str:
        return str(self.__dict__)

    def __str__(self) -> str:
        return str(self.__dict__)
