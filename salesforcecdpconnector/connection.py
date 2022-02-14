#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
from .authentication_helper import AuthenticationHelper
from .constants import API_VERSION_V2
from .cursor import SalesforceCDPCursor
from .exceptions import Error
from .pandas_utils import PandasUtils

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"


class SalesforceCDPConnection:
    """
    This object represents a connection to CDP
    """

    def __init__(self, login_url, username=None, password=None, client_id=None, client_secret=None,
                 api=API_VERSION_V2, core_token=None, refresh_token=None):
        self.login_url = login_url
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.api = api
        self.core_token = core_token
        self.refresh_token = refresh_token
        self.closed = False
        self.authentication_helper = AuthenticationHelper(self)

    def cursor(self):
        """
        :return: Returns a new instance of the SalesforceCDPCursor
        """
        if self.closed:
            raise Error('Cannot create cursor. Connection is closed')
        return SalesforceCDPCursor(self)

    def get_pandas_dataframe(self, query):
        """
        Returns the query result as Pandas Dataframe
        :param query: The input query
        :return: Query Results as Pandas dataframe
        """
        if self.closed:
            raise Error('Cannot create dataframe. Connection is closed')
        return PandasUtils.get_dataframe(self, query)

    def close(self):
        """
        Clearing credentials
        Marking connection as closed
        :return: None
        """
        self.login_url = None
        self.username = None
        self.password = None
        self.client_id = None
        self.client_secret = None
        self.core_token = None
        self.refresh_token = None
        self.authentication_helper = None
        self.closed = True

    def commit(self):
        """
        Nothing to commit
        :return: None
        """
        pass

    def rollback(self):
        """
        Nothing to rollback
        :return: None
        """
        pass
