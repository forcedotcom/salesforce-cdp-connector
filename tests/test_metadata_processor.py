#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import unittest
from unittest.mock import patch

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
                        "name": "city__c",
                        "displayName": "city",
                        "type": "STRING"
                    },
                    {
                        "name": "created__c",
                        "displayName": "created",
                        "type": "DATE_TIME"
                    }
                ],
                "category": "Profile",
                "name": "Anahita_runner_profiles_D88F0219__dlm",
                "displayName": "Anahita-runner_profiles",
                "primaryKeys": [
                    {
                        "name": "city__c",
                        "displayName": "city",
                        "indexOrder": "1"
                    }
                ]
            },
            {
                "fields": [
                    {
                        "name": "cdp_sys_SourceVersion__c",
                        "displayName": "cdp_sys_SourceVersion",
                        "type": "STRING"
                    },
                    {
                        "name": "id__c",
                        "displayName": "id",
                        "type": "STRING"
                    },
                    {
                        "name": "modified__c",
                        "displayName": "modified",
                        "type": "DATE_TIME"
                    },
                    {
                        "name": "name__c",
                        "displayName": "name",
                        "type": "STRING"
                    }
                ],
                "category": "Related",
                "name": "AniketNTO_footwear_0BC0C938__dlo",
                "displayName": "AniketNTO-footwear",
                "primaryKeys": [
                    {
                        "name": "id__c",
                        "displayName": "id",
                        "indexOrder": "1"
                    }
                ]
            }
        ]
    }

    entity_name_result = {
        "fields": [
            {
                "name": "cdp_sys_SourceVersion__c",
                "displayName": "cdp_sys_SourceVersion",
                "type": "STRING"
            },
            {
                "name": "id__c",
                "displayName": "id",
                "type": "STRING"
            },
            {
                "name": "modified__c",
                "displayName": "modified",
                "type": "DATE_TIME"
            },
            {
                "name": "name__c",
                "displayName": "name",
                "type": "STRING"
            }
        ],
        "category": "Related",
        "name": "AniketNTO_footwear_0BC0C938__dlo",
        "displayName": "AniketNTO-footwear",
        "primaryKeys": [
            {
                "name": "id__c",
                "displayName": "id",
                "indexOrder": "1"
            }
        ]
    }

    @patch.object(QuerySubmitter, 'get_metadata', return_value=metadata_result)
    def test_get_metadata(self, mock1):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        table_list = connection.get_tables()
        table_metadata_by_entity_name = connection.describe_table('AniketNTO-footwear')
        table_metadata_by_entity_category = connection.describe_table('', 'Profile')
        table_metadata_by_entity_type = connection.describe_table('', '', 'dlm')
        table_metadata_complete = connection.describe_table()
        metadata_table_list = ['AccountContact', 'ACX_DelTable', 'Anahita-exercises', 'Anahita-runner_profiles', 'AniketNTO-footwear']

        # validating the results of all the tests
        self.assertListEqual(table_list, metadata_table_list)
        self.assertEqual(table_metadata_by_entity_name, MyTestCase.entity_name_result)
        self.assertEqual(len(table_metadata_by_entity_category), 3)
        self.assertEqual(len(table_metadata_by_entity_type), 2)
        self.assertEqual(len(table_metadata_complete['metadata']), 5)

        # executing negative test cases
        table_metadata_by_entity_name_false = connection.describe_table('InvalidTable')
        table_metadata_by_entity_category_false = connection.describe_table('', 'InvalidCategory')
        table_metadata_by_entity_type_false = connection.describe_table('', '', 'InvalidType')

        self.assertEqual(table_metadata_by_entity_name_false, None)
        self.assertEqual(len(table_metadata_by_entity_category_false), 0)
        self.assertEqual(len(table_metadata_by_entity_type_false), 0)


if __name__ == '__main__':
    unittest.main()
