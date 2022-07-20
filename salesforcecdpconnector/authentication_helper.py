#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from .constants import AUTH_PARAM_GRANT_TYPE
from .constants import AUTH_PARAM_CDP_GRANT_TYPE
from .constants import AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE
from .constants import AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE_VALUE
from .constants import AUTH_PARAM_CDP_SUBJECT_TOKEN
from .constants import AUTH_RESPONSE_ACCESS_TOKEN
from .constants import AUTH_RESPONSE_EXPIRES_IN
from .constants import AUTH_RESPONSE_INSTANCE_URL
from .constants import AUTH_PARAM_REFRESH_TOKEN_GRANT_TYPE
from .constants import AUTH_PARAM_CLIENT_ID
from .constants import AUTH_PARAM_CLIENT_SECRET
from .constants import AUTH_PARAM_P_D
from .constants import AUTH_PARAM_USERNAME
from .exceptions import Error
from datetime import datetime, timedelta
from threading import Lock

import requests


class AuthenticationHelper:

    def __init__(self, connection):
        self.exchange_token = None
        self.instance_url = None
        self.token_expiry_time = None
        self.connection = connection
        self.lock = Lock()

    def get_token(self):
        """
        Retrieves the cdp token and instance url.
        The connection should be having client_id, client_secret and either refresh token or username and password

        :return: cdp token, instance_url
        """
        if self._is_token_valid():
            return self.exchange_token, self.instance_url
        return self._get_token()

    def _get_token(self):
        """
        Retrieves the cdp token and instance url. This method does is synchronized.
        The connection should be having client_id, client_secret and either refresh token or username and password

        :return: cdp token, instance_url
        """
        try:
            self.lock.acquire()
            if self._is_token_valid():
                return self.exchange_token, self.instance_url
            if self.connection.refresh_token is not None:
                if self.connection.core_token is not None:
                    try:
                        return self._exchange_token(self.connection.login_url, self.connection.core_token)
                    except Exception:
                        # core token might be expired
                        # try renewing token
                        pass

                return self._renew_token(self.connection.login_url, self.connection.refresh_token,
                                         self.connection.client_id, self.connection.client_secret)
            elif self.connection.password is not None:
                return self._token_by_un_pwd_flow(self.connection.login_url, self.connection.client_id,
                                                  self.connection.client_secret, self.connection.username,
                                                  self.connection.password)
            else:
                raise Error('Sufficient information is not available for authentication')
        finally:
            self.lock.release()

    def _is_token_valid(self):
        """
        Checks if the token is valid

        :return: True if the token is valid
        """
        if self.token_expiry_time is not None and self.exchange_token is not None:
            current_time = datetime.now()
            return current_time < self.token_expiry_time
        return False

    def _exchange_token(self, login_url, core_token):
        params = {AUTH_PARAM_GRANT_TYPE: AUTH_PARAM_CDP_GRANT_TYPE,
                  AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE: AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE_VALUE,
                  AUTH_PARAM_CDP_SUBJECT_TOKEN: core_token}
        current_time = datetime.now()
        access_code_res = requests.post(url=login_url + '/services/a360/token', params=params)
        if access_code_res.status_code == 200:
            access_code = access_code_res.json()
            access_token = access_code[AUTH_RESPONSE_ACCESS_TOKEN]
            expires_in_seconds = access_code[AUTH_RESPONSE_EXPIRES_IN]
            instance_url = access_code[AUTH_RESPONSE_INSTANCE_URL]
            token_expiry_time = current_time + timedelta(seconds=expires_in_seconds)
            AuthenticationHelper._revoke_core_token(login_url, core_token)
        else:
            raise Error('CDP token retrieval failed with code %d' % access_code_res.status_code)
        self.exchange_token = access_token
        self.token_expiry_time = token_expiry_time
        self.instance_url = instance_url
        return access_token, instance_url

    @staticmethod
    def _revoke_core_token(login_url, core_token):
        """
        Revokes the core token
        :param login_url: The login URL
        :param core_token: The core token
        :return:
        """
        params = {'token': core_token}
        access_code_res = requests.post(url=login_url + '/services/oauth2/revoke', params=params)
        if access_code_res.status_code != 200:
            raise Error('Core token revoke failed with code %d' % access_code_res.status_code)

    def _renew_token(self, login_url, refresh_token, client_id, client_secret):
        """
        Revives the expired token

        :param login_url: The login URL
        :param refresh_token: The OAuth refresh token
        :param client_id: The client id for the app
        :param client_secret: The client secret for the app
        :return: cdp_token, instance_url will be returned
        """
        params = {AUTH_PARAM_GRANT_TYPE: AUTH_PARAM_REFRESH_TOKEN_GRANT_TYPE,
                  AUTH_PARAM_CLIENT_ID: client_id,
                  AUTH_PARAM_CLIENT_SECRET: client_secret,
                  AUTH_PARAM_REFRESH_TOKEN_GRANT_TYPE: refresh_token}
        access_code_res = requests.post(url=login_url + '/services/oauth2/token', params=params)
        if access_code_res.status_code == 200:
            access_code = access_code_res.json()
            core_token = access_code[AUTH_RESPONSE_ACCESS_TOKEN]
            org_url = access_code[AUTH_RESPONSE_INSTANCE_URL]
            return self._exchange_token(org_url, core_token)
        else:
            raise Error('Token Renewal failed with code %d' % access_code_res.status_code)

    def _token_by_un_pwd_flow(self, login_url, client_id, client_secret, username, password):
        """
        This function fetches the core token
        :param login_url: The Login URL for the tenant
        :param client_id: The client id for the app
        :param client_secret: The client secret for the app
        :param username: Tenant username
        :param password: Tenant password
        :return: cdp_token, instance_url will be returned
        """
        params = {AUTH_PARAM_GRANT_TYPE: AUTH_PARAM_P_D, AUTH_PARAM_CLIENT_ID: client_id,
                  AUTH_PARAM_CLIENT_SECRET: client_secret,
                  AUTH_PARAM_USERNAME: username, AUTH_PARAM_P_D: password}
        access_code_res = requests.post(url=login_url + '/services/oauth2/token', params=params)
        if access_code_res.status_code == 200:
            access_code = access_code_res.json()
            core_token = access_code[AUTH_RESPONSE_ACCESS_TOKEN]
            org_url = access_code[AUTH_RESPONSE_INSTANCE_URL]
            return self._exchange_token(org_url, core_token)
        else:
            raise Error('Core token retrieval failed with code %d' % access_code_res.status_code)
