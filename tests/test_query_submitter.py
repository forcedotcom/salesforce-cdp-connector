#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import json
import re

import responses
import unittest

from salesforcecdpconnector.query_submitter import QuerySubmitter


class TestQuerySubmitter(unittest.TestCase):
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

    call2 = {
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
        "done": True,
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

    @responses.activate
    def test_get_query_results(self):
        responses.add(**{
            'method': responses.POST,
            'url': re.compile('https://www.salesforce.com.*'),
            'body': json.dumps(self.call1),
            'status': 200
        })

        results = QuerySubmitter._get_query_results('select * from UnifiedIndividuals__dlm',
                                                    'www.salesforce.com', 'token')

        self.assertEqual(len(results['data']), 3)  # add assertion here

    @responses.activate
    def test_get_next_batch(self):
        responses.add(**{
            'method': responses.GET,
            'url': re.compile('https://www.salesforce.com.*'),
            'body': json.dumps(self.call2),
            'status': 200
        })

        results = QuerySubmitter._get_next_batch_results('fa489494-ff42-45ce-afd6-b838854b5a99',
                                                         'www.salesforce.com', 'token')

        self.assertEqual(len(results['data']), 3)  # add assertion here


if __name__ == '__main__':
    unittest.main()
