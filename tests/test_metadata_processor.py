#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import unittest
from unittest.mock import patch
from salesforcecdpconnector.genie_table import *

from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.query_submitter import QuerySubmitter


class MyTestCase(unittest.TestCase):
    metadata_result = {
        "metadata": [
            {
                "fields": [
                    {
                        "name": "accountcontact__c",
                        "displayName": "AccountContact",
                        "type": "STRING"
                    },
                    {
                        "name": "actioncadencestep__c",
                        "displayName": "ActionCadenceStep",
                        "type": "STRING"
                    }
                ],
                "category": "Profile",
                "name": "abc__dll",
                "displayName": "AccountContact",
                "primaryKeys": [
                    {
                        "name": "accountcontact__c",
                        "displayName": "AccountContact",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "birthday__c",
                        "displayName": "birthday",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "id__c",
                        "displayName": "id",
                        "type": "NUMBER"
                    }
                ],
                "category": "Profile",
                "name": "ACX_DelTable__dll",
                "displayName": "ACX_DelTable",
                "primaryKeys": [
                    {
                        "name": "id__c",
                        "displayName": "id",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "calories_burned__c",
                        "displayName": "calories_burned",
                        "type": "NUMBER"
                    },
                    {
                        "name": "runid__c",
                        "displayName": "runid",
                        "type": "NUMBER"
                    },
                    {
                        "name": "type__c",
                        "displayName": "type",
                        "type": "STRING"
                    }
                ],
                "category": "Engagement",
                "name": "Anahita_exercises_6174646D__dlm",
                "displayName": "Anahita-exercises",
                "primaryKeys": [
                    {
                        "name": "runid__c",
                        "displayName": "runid",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "Brand_SM_PID__dlm",
                "displayName": "Brand - Profiles",
                "relationships": [
                    {
                        "fromEntity": "Brand_SM_PID__dlm",
                        "toEntity": "ssot__Brand__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "ssot__Id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "NUMBER"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "SrcValue_SM_PID__dlm",
                "displayName": "SrcValue - Profiles",
                "relationships": [
                    {
                        "fromEntity": "SrcValue_SM_PID__dlm",
                        "toEntity": "SrcValue__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "name": "yrOpr1__cio",
                "displayName": "yrOpr1",
                "dimensions": [
                    {
                        "name": "indbirth__c",
                        "displayName": "",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "ids__c",
                        "displayName": "",
                        "type": "STRING"
                    }
                ],
                "measures": [
                    {
                        "name": "cids__c",
                        "displayName": "",
                        "type": "NUMBER"
                    }
                ],
                "relationships": [
                    {
                        "fromEntity": "ssot__Individual__dlm",
                        "toEntity": "yrOpr1__cio"
                    }
                ],
                "partitionBy": "indbirth__c",
                "latestProcessTime": "2023-01-18T12:06:19.95000Z",
                "latestSuccessfulProcessTime": "2023-01-18T10:00:57.00000Z"
            }
        ]
    }

    metadata_dmo = {
        "metadata": [
            {
                "fields": [
                    {
                        "name": "calories_burned__c",
                        "displayName": "calories_burned",
                        "type": "NUMBER"
                    },
                    {
                        "name": "runid__c",
                        "displayName": "runid",
                        "type": "NUMBER"
                    },
                    {
                        "name": "type__c",
                        "displayName": "type",
                        "type": "STRING"
                    }
                ],
                "category": "Engagement",
                "name": "Anahita_exercises_6174646D__dlm",
                "displayName": "Anahita-exercises",
                "primaryKeys": [
                    {
                        "name": "runid__c",
                        "displayName": "runid",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "Brand_SM_PID__dlm",
                "displayName": "Brand - Profiles",
                "relationships": [
                    {
                        "fromEntity": "Brand_SM_PID__dlm",
                        "toEntity": "ssot__Brand__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "ssot__Id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "NUMBER"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "SrcValue_SM_PID__dlm",
                "displayName": "SrcValue - Profiles",
                "relationships": [
                    {
                        "fromEntity": "SrcValue_SM_PID__dlm",
                        "toEntity": "SrcValue__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            }
        ]
    }

    metadata_segment_membership = {
        "metadata": [
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "Brand_SM_PID__dlm",
                "displayName": "Brand - Profiles",
                "relationships": [
                    {
                        "fromEntity": "Brand_SM_PID__dlm",
                        "toEntity": "ssot__Brand__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "ssot__Id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "NUMBER"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "SrcValue_SM_PID__dlm",
                "displayName": "SrcValue - Profiles",
                "relationships": [
                    {
                        "fromEntity": "SrcValue_SM_PID__dlm",
                        "toEntity": "SrcValue__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            }
        ]
    }

    metadata_entity_name = {
        "metadata": [
            {
                "fields": [
                    {
                        "name": "Delta_Type__c",
                        "displayName": "Delta Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "type": "NUMBER"
                    },
                    {
                        "name": "Segment_Id__c",
                        "displayName": "Segment Id",
                        "type": "STRING"
                    },
                    {
                        "name": "Snapshot_Type__c",
                        "displayName": "Sanpshot Type",
                        "type": "STRING"
                    },
                    {
                        "name": "Timestamp__c",
                        "displayName": "Timestamp",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "Version_Stamp__c",
                        "displayName": "Version Stamp",
                        "type": "DATE_TIME"
                    }
                ],
                "indexes": [],
                "category": "Segment_Membership",
                "name": "SrcValue_SM_PID__dlm",
                "displayName": "SrcValue - Profiles",
                "relationships": [
                    {
                        "fromEntity": "SrcValue_SM_PID__dlm",
                        "toEntity": "SrcValue__dlm",
                        "fromEntityAttribute": "Id__c",
                        "toEntityAttribute": "id__c",
                        "cardinality": "NTOONE"
                    }
                ],
                "primaryKeys": [
                    {
                        "name": "Id__c",
                        "displayName": "Id",
                        "indexOrder": "1"
                    }
                ]
            }
        ]
    }

    table_entry_1 = GenieTable(name='abc__dll', display_name='AccountContact', category='Profile',
                               primary_keys=[PrimaryKeys('accountcontact__c', 'AccountContact', '1')],
                               partition_by=None, fields=[Field('accountcontact__c', 'AccountContact', 'STRING'),
                                                        Field('actioncadencestep__c', 'ActionCadenceStep',
                                                              'STRING')],
                               relationships=[],
                               indexes=[])
    table_entry_2 = GenieTable(name='ACX_DelTable__dll', display_name='ACX_DelTable', category='Profile',
                               primary_keys=[PrimaryKeys('id__c', 'id', '1')],
                               partition_by=None, fields=[Field('birthday__c', 'birthday', 'DATE_TIME'),
                                                        Field('id__c', 'id', 'NUMBER')],
                               relationships=[],
                               indexes=[])
    table_entry_3 = GenieTable(name='Anahita_exercises_6174646D__dlm', display_name='Anahita-exercises',
                               category='Engagement',
                               primary_keys=[PrimaryKeys('runid__c', 'runid', '1')],
                               partition_by=None, fields=[Field('calories_burned__c', 'calories_burned', 'NUMBER'),
                                                        Field('runid__c', 'runid', 'NUMBER'),
                                                        Field('type__c', 'type', 'STRING')],
                               relationships=[],
                               indexes=[])
    table_entry_4 = GenieTable(name='Brand_SM_PID__dlm', display_name='Brand - Profiles',
                               category='Segment_Membership',
                               primary_keys=[PrimaryKeys('Id__c', 'Id', '1')],
                               partition_by=None, fields=[Field('Delta_Type__c', 'Delta Type', 'STRING'),
                                                        Field('Id__c', 'Id', 'STRING'),
                                                        Field('Segment_Id__c', 'Segment Id', 'STRING'),
                                                        Field('Snapshot_Type__c', 'Sanpshot Type', 'STRING'),
                                                        Field('Timestamp__c', 'Timestamp', 'DATE_TIME'),
                                                        Field('Version_Stamp__c', 'Version Stamp', 'DATE_TIME')],
                               relationships=[
                                   Relationship('Brand_SM_PID__dlm', 'ssot__Brand__dlm', 'Id__c', 'ssot__Id__c',
                                                'NTOONE')],
                               indexes=[])
    table_entry_5 = GenieTable(name='SrcValue_SM_PID__dlm', display_name='SrcValue - Profiles',
                               category='Segment_Membership',
                               primary_keys=[PrimaryKeys('Id__c', 'Id', '1')],
                               partition_by=None, fields=[Field('Delta_Type__c', 'Delta Type', 'STRING'),
                                                        Field('Id__c', 'Id', 'NUMBER'),
                                                        Field('Segment_Id__c', 'Segment Id', 'STRING'),
                                                        Field('Snapshot_Type__c', 'Sanpshot Type', 'STRING'),
                                                        Field('Timestamp__c', 'Timestamp', 'DATE_TIME'),
                                                        Field('Version_Stamp__c', 'Version Stamp', 'DATE_TIME')],
                               relationships=[
                                   Relationship('SrcValue_SM_PID__dlm', 'SrcValue__dlm', 'Id__c', 'id__c',
                                                'NTOONE')],
                               indexes=[])
    table_entry_6 = GenieTable(name='yrOpr1__cio', display_name='yrOpr1', category=None,
                               primary_keys=[],
                               partition_by='indbirth__c',
                               fields=[Field('indbirth__c', '', 'DATE_TIME', False, True),
                                       Field('ids__c', '', 'STRING', False, True),
                                       Field('cids__c', '', 'NUMBER', True, False)],
                               relationships=[Relationship('ssot__Individual__dlm', 'yrOpr1__cio')], indexes=[])

    @patch.object(QuerySubmitter, 'get_metadata', return_value=metadata_result)
    def test_get_list_tables(self, mock1):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        genie_table_list_expected = [self.table_entry_1, self.table_entry_2, self.table_entry_3, self.table_entry_4,
                                     self.table_entry_5,
                                     self.table_entry_6]
        genie_table_list_returned = connection.list_tables()
        for (entry1, entry2) in zip(genie_table_list_expected, genie_table_list_returned):
            self.assertEqual(entry1, entry2)

    @patch.object(QuerySubmitter, 'get_metadata', return_value=metadata_entity_name)
    def test_get_list_tables_with_entity_name(self, mock1):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        genie_table_list_returned_with_table_name = connection.list_tables('SrcValue_SM_PID__dlm')
        self.assertEqual(genie_table_list_returned_with_table_name[0], self.table_entry_5)

    @patch.object(QuerySubmitter, 'get_metadata', return_value=metadata_segment_membership)
    def test_get_list_tables_with_entity_category(self, mock1):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        genie_table_list_returned_with_table_category = connection.list_tables(table_name=None,
                                                                           table_category='Segment_Membership',
                                                                           table_type=None)
        self.assertEqual(genie_table_list_returned_with_table_category, [self.table_entry_4, self.table_entry_5])

    @patch.object(QuerySubmitter, 'get_metadata', return_value=metadata_dmo)
    def test_get_list_tables_with_entity_type(self, mock1):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        genie_table_list_returned_with_table_type = connection.list_tables(table_name=None,
                                                                           table_category=None,
                                                                           table_type='DataModelObject')
        self.assertEqual(genie_table_list_returned_with_table_type, 
                         [self.table_entry_3, self.table_entry_4, self.table_entry_5])


if __name__ == '__main__':
    unittest.main()
