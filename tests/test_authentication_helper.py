#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

import json
import re
import unittest
from unittest.mock import MagicMock

import responses

from salesforcecdpconnector.authentication_helper import AuthenticationHelper
from salesforcecdpconnector.connection import SalesforceCDPConnection

from cryptography.hazmat.primitives.asymmetric import rsa


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

    def _generate_public_private_key_pair(self):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        return (public_key, private_key)

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
    
    @responses.activate
    def test_token_by_jwt_bearer_flow(self):
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

        (public_key, private_key) = self._generate_public_private_key_pair()

        connection = SalesforceCDPConnection(login_url='https://login.salesforce.com',  
                                            client_id='clientId', 
                                            username='username', 
                                            private_key=private_key)

        authenticationHelper = AuthenticationHelper(connection)
        token, instanceUrl = authenticationHelper.get_token()

        self.assertEqual(token, 'access_token')
        self.assertEqual(instanceUrl, 'instanceurl.salesforce.com')

    def test_retry(self):
        connection = SalesforceCDPConnection('https://login.salesforce.com', 'username', 'password', 'clientId',
                                             'clientSecret')
        authenticationHelper = AuthenticationHelper(connection)
        authenticationHelper._get_token = MagicMock(side_effect=ValueError())
        try:
            authenticationHelper.get_token()
        except:
            pass
        self.assertEqual(authenticationHelper._get_token.call_count, 3)


if __name__ == '__main__':
    unittest.main()
