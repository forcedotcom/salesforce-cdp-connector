#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

from datetime import timedelta
import json
import logging
import requests
import socket
from timeit import default_timer as timer

from .constants import API_VERSION_V2
from .constants import API_VERSION_V1
from .constants import QUERY_HEADER_KEY_AUTHORIZATION
from .constants import QUERY_HEADER_KEY_CONTENT_TYPE
from .constants import QUERY_HEADER_VALUE_APPLICATION_JSON
from .constants import QUERY_HEADER_KEY_ACCEPT_ENCODING
from .constants import QUERY_HEADER_VALUE_GZIP
from .exceptions import Error


def allowed_gai_family():
    return socket.AF_INET


requests.packages.urllib3.util.connection.allowed_gai_family = allowed_gai_family


class QuerySubmitter:
    """
    Helper methods to execute query against V2 API
    """

    logger = logging.getLogger()
    session = requests.session()

    @staticmethod
    def execute(connection, query, api_version=API_VERSION_V2, enable_arrow_stream=False):
        """
        This method submits the query to queryV2 API for execution
        :param connection: SalesforceCDPConnection
        :param query: The query to be executed
        :param api_version: v1 or v2 API
        :param enable_arrow_stream: Set as True to fetch the results as ArrowStream
        :return: Returns the response JSON
        """
        token, instance_url = connection.authentication_helper.get_token()
        return QuerySubmitter._get_query_results(query, instance_url, token, api_version, enable_arrow_stream)

    @staticmethod
    def get_next_batch(connection, next_batch_id, enable_arrow_stream=False):
        """
        This method fetches the next batch of results using the v2 APIs.
        :param connection:  SalesforceCDPConnection
        :param next_batch_id: batchId to fetch the results
        :param enable_arrow_stream: Set as True to fetch the results as ArrowStream
        :return:
        """
        token, instance_url = connection.authentication_helper.get_token()
        return QuerySubmitter._get_next_batch_results(next_batch_id, instance_url, token, enable_arrow_stream)

    @staticmethod
    def get_metadata(connection, request_params={}):
        """
        This method fetches the metadata for a given tenant.
        :param connection:  SalesforceCDPConnection
        :return: Metadata for a given tenant
        """
        token, instance_url = connection.authentication_helper.get_token()
        return QuerySubmitter.__get_metadata_results(instance_url, token, request_params)

    @staticmethod
    def _get_query_results(query, instance_url, token, api_version='V2', enable_arrow_stream=False):
        url = f'https://{instance_url}/api/{api_version}/query'
        json_payload = QuerySubmitter._get_payload(query)
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        QuerySubmitter.logger.debug("Submitting query for execution")
        start_time = timer()
        sql_response = QuerySubmitter.session.post(url=url, data=json_payload, headers=headers, verify=False)
        QuerySubmitter.logger.debug("Query Submitted in %s", str(timedelta(seconds=timer() - start_time)))
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing query in server : %s' % error_message)
                raise Error('Failed executing query in server')
        response_json = sql_response.json()
        return response_json

    @staticmethod
    def _get_next_batch_results(next_batch_id, instance_url, token, enable_arrow_stream=False):
        url = f'https://{instance_url}/api/v2/query/{next_batch_id}'
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        start_time = timer()
        sql_response = QuerySubmitter.session.get(url=url, headers=headers, verify=False)
        QuerySubmitter.logger.debug("Fetched next batch in %s", str(timedelta(seconds=timer() - start_time)))
        response_json = sql_response.json()
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing query in server : %s' % error_message)
                raise Error('Failed executing query in server')
        return response_json

    @staticmethod
    def _get_headers(token, enable_arrow_stream):
        headers = {QUERY_HEADER_KEY_AUTHORIZATION: f'Bearer {token}',
                   QUERY_HEADER_KEY_CONTENT_TYPE: QUERY_HEADER_VALUE_APPLICATION_JSON,
                   QUERY_HEADER_KEY_ACCEPT_ENCODING: QUERY_HEADER_VALUE_GZIP}
        if enable_arrow_stream:
            headers['enable-arrow-stream'] = 'true'
        return headers

    @staticmethod
    def __get_metadata_results(instance_url, token, parameters={}):
        url = f'https://{instance_url}/api/v1/metadata'
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream=False)
        QuerySubmitter.logger.debug("Submitting metadata query for execution")
        start_time = timer()
        sql_response = QuerySubmitter.session.get(url=url, headers=headers, verify=False, params=parameters)
        QuerySubmitter.logger.debug("Metadata Query Submitted in %s", str(timedelta(seconds=timer() - start_time)))
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing metadata query in server : %s' % error_message)
                raise Error('Failed executing query in server')
        response_json = sql_response.json()
        return response_json

    @staticmethod
    def _get_payload(query):
        payload = {
            'sql': query
        }
        json_payload = json.dumps(payload)
        return json_payload
