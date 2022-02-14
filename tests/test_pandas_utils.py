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

    call1 = {
        "startTime": "2022-03-08T09:14:24.089261Z",
        "endTime": "2022-03-08T09:14:30.044897Z",
        "rowCount": 3,
        "queryId": "20220308_091425_01733_5frjj",
        "nextBatchId": "408e58c2-3a92-40bc-85e9-6356556185b2",
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
        },
        "arrowStream": "/////9gAAAAQAAAAAAAKAA4ABgANAAgACgAAAAAABAAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAIAAABcAAAABAAAAL7///8UAAAAFAAAABQAAAAAAAUBEAAAAAAAAAAAAAAArP///xkAAABzc290X19MYXN0TW9kaWZpZWREYXRlX19jABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABgAAAAAAAUBFAAAAAAAAAAAAAAABAAEAAQAAAASAAAAc3NvdF9fRmlyc3ROYW1lX19jAAD/////2AAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAAIgAAAAAAAAAAAAEABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAAHgAAAADAAAAAAAAAAAAAAAGAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAEAAAAAAAAAAYAAAAAAAAAAwAAAAAAAAAKAAAAAAAAAABAAAAAAAAADAAAAAAAAAAEAAAAAAAAABAAAAAAAAAAEgAAAAAAAAAAAAAAAIAAAADAAAAAAAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAcAAAAAAAAAAAAAAAQAAAAHAAAADAAAAEFuZHlKb25TYXJhaAAAAAAHAAAAAAAAAAAAAAAYAAAAMAAAAEgAAAAyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFoyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFoyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFr/////AAAAAA=="
    }

    call2 = {
        "startTime": "2022-03-08T09:14:24.089261Z",
        "endTime": "2022-03-08T09:14:30.044897Z",
        "rowCount": 3,
        "queryId": "20220308_091425_01733_5frjj",
        "nextBatchId": "408e58c2-3a92-40bc-85e9-6356556185b2",
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
        },
        "arrowStream": "/////9gAAAAQAAAAAAAKAA4ABgANAAgACgAAAAAABAAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAIAAABcAAAABAAAAL7///8UAAAAFAAAABQAAAAAAAUBEAAAAAAAAAAAAAAArP///xkAAABzc290X19MYXN0TW9kaWZpZWREYXRlX19jABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABgAAAAAAAUBFAAAAAAAAAAAAAAABAAEAAQAAAASAAAAc3NvdF9fRmlyc3ROYW1lX19jAAD/////2AAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAAIgAAAAAAAAAAAAEABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAAHgAAAADAAAAAAAAAAAAAAAGAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAEAAAAAAAAAAYAAAAAAAAAAwAAAAAAAAAKAAAAAAAAAABAAAAAAAAADAAAAAAAAAAEAAAAAAAAABAAAAAAAAAAEgAAAAAAAAAAAAAAAIAAAADAAAAAAAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAcAAAAAAAAAAAAAAAQAAAAHAAAADAAAAEFuZHlKb25TYXJhaAAAAAAHAAAAAAAAAAAAAAAYAAAAMAAAAEgAAAAyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFoyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFoyMDIxLTA5LTE2VDE2OjI2OjM2LjAwMFr/////AAAAAA=="
    }

    @patch.object(QuerySubmitter, 'get_next_batch', return_value=call2)
    @patch.object(QuerySubmitter, 'execute', return_value=call1)
    def test_get_dataframe(self, mock1, mock2):
        connection = SalesforceCDPConnection('url', 'username', 'password', 'client_id', 'client_secret')
        dataframe = connection.get_pandas_dataframe('select * from UnifiedIndividuals__dlm')
        self.assertEqual(len(dataframe), 6)  # add assertion here
        self.assertListEqual(dataframe.columns.tolist(), ['ssot__FirstName__c', 'ssot__LastModifiedDate__c'])
        self.assertEqual(dataframe.dtypes['ssot__LastModifiedDate__c'].base.name, 'datetime64[ns]')





if __name__ == '__main__':
    unittest.main()
