#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import unittest

from salesforcecdpconnector.query_result_parser import QueryResultParser


class TestQueryResultParser(unittest.TestCase):
    call1 = {
        "data": [
            [
                "Andy",
                "2021-09-16T16:26:36.000+00:00"
            ],
            [
                "Jon",
                "2021-09-16T16:26:36.000+00:00"
            ],
            [
                "Sarah",
                "2021-09-16T16:26:36.000+00:00"
            ]
        ],
        "startTime": "2022-03-07T19:57:19.374525Z",
        "endTime": "2022-03-07T19:57:20.063372Z",
        "rowCount": 3,
        "queryId": "20220307_195719_00109_5frjj",
        "nextBatchId": "fa489494-ff42-45ce-afd6-b838854b5a99",
        "done": False,
        "metadata": {
            "ssot__FirstName__c": {
                "type": "VARCHAR",
                "placeInOrder": 0,
                "typeCode": 12
            },
            "ssot__LastModifiedDate__c": {
                "type": "TIMESTAMP",
                "placeInOrder": 1,
                "typeCode": 93
            }
        }
    }

    def test_parsing(self):
        parsed_result = QueryResultParser.parse_result(self.call1)
        columns_from_description = [x[0] for x in parsed_result.description]
        self.assertEqual(columns_from_description, ['ssot__FirstName__c', 'ssot__LastModifiedDate__c'])
        firstname_data = [x[0] for x in parsed_result.data]
        self.assertEqual(firstname_data, ['Andy', 'Jon', 'Sarah'])
        column_types_from_description = [x[1] for x in parsed_result.description]
        self.assertEqual(column_types_from_description, ['VARCHAR', 'TIMESTAMP'])
        self.assertTrue(parsed_result.has_next)
        self.assertIsNotNone(parsed_result.next_batch_id)


if __name__ == '__main__':
    unittest.main()
