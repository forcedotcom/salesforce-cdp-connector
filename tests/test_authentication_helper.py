#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import json
import re
import unittest

import responses

from salesforcecdpconnector.authentication_helper import AuthenticationHelper
from salesforcecdpconnector.connection import SalesforceCDPConnection


class TestAuthenticationHelper(unittest.TestCase):
    core_response = {
        "access_token": "access_token",
        "instance_url": "https://someorgurl.salesforce.com",
        "id": "someid",
        "token_type": "Bearer",
        "issued_at": "1653555555555",
        "signature": "somesignature"
    }

    exchange_response = {
        "access_token": "access_token",
        "instance_url": "instanceurl.salesforce.com",
        "token_type": "Bearer",
        "issued_token_type": "tokentype",
        "expires_in": 1000
    }

    @responses.activate
    def test_token_by_un_pwd_flow(self):
        responses.add(**{
            'method': responses.POST,
            'url': re.compile('https://login.salesforce.com/*'),
            'body': json.dumps(self.core_response),
            'status': 200
        })
        responses.add(**{
            'method': responses.POST,
            'url': re.compile('https://someorgurl.salesforce.com/*'),
            'body': json.dumps(self.exchange_response),
            'status': 200
        })

        connection = SalesforceCDPConnection('https://login.salesforce.com', 'username', 'password', 'clientId', 'clientSecret')
        authenticationHelper = AuthenticationHelper(connection)
        token, instanceUrl = authenticationHelper.get_token()

        self.assertEqual(token, 'access_token')
        self.assertEqual(instanceUrl, 'instanceurl.salesforce.com')

if __name__ == '__main__':
    unittest.main()
