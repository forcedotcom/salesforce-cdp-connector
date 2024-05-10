#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

API_VERSION_V1 = "v1"
API_VERSION_V2 = "v2"

AUTH_PARAM_GRANT_TYPE = 'grant_type'
AUTH_PARAM_P_D = 'password'
AUTH_PARAM_CLIENT_ID = 'client_id'
AUTH_PARAM_CLIENT_SECRET = 'client_secret'
AUTH_PARAM_USERNAME = 'username'
AUTH_PARAM_ASSERTION = 'assertion'
AUTH_RESPONSE_ACCESS_TOKEN = 'access_token'
AUTH_RESPONSE_EXPIRES_IN = 'expires_in'
AUTH_RESPONSE_INSTANCE_URL = 'instance_url'

AUTH_PARAM_JWT_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:jwt-bearer'
AUTH_PARAM_CDP_GRANT_TYPE = 'urn:salesforce:grant-type:external:cdp'
AUTH_PARAM_CDP_SUBJECT_TOKEN = 'subject_token'
AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE = 'subject_token_type'
AUTH_PARAM_CDP_SUBJECT_TOKEN_TYPE_VALUE = 'urn:ietf:params:oauth:token-type:access_token'
AUTH_PARAM_REFRESH_TOKEN_GRANT_TYPE = 'refresh_token'

ENCODING_ASCII = 'ascii'

DATA_TYPE_DECIMAL = 'DECIMAL'
DATA_TYPE_TIMESTAMP = 'TIMESTAMP'
DATA_TYPE_TIMESTAMP_WITH_TIMEZONE = 'TIMESTAMP WITH TIME ZONE'

QUERY_RESPONSE_KEY_ARROW_STREAM = 'arrowStream'
QUERY_RESPONSE_KEY_DATA = 'data'
QUERY_RESPONSE_KEY_METADATA = 'metadata'
QUERY_RESPONSE_KEY_METADATA_TYPE = 'type'
QUERY_RESPONSE_KEY_DONE = 'done'
QUERY_RESPONSE_KEY_NEXT_BATCH_ID = 'nextBatchId'
QUERY_RESPONSE_KEY_PLACE_IN_ORDER = 'placeInOrder'

QUERY_HEADER_KEY_AUTHORIZATION = 'Authorization'
QUERY_HEADER_KEY_CONTENT_TYPE = 'Content-Type'
QUERY_HEADER_KEY_ACCEPT_ENCODING = 'Accept-Encoding'
QUERY_HEADER_VALUE_APPLICATION_JSON = 'application/json'
QUERY_HEADER_VALUE_GZIP = 'gzip'

MAX_RETRY_COUNT = 3
RETRY_DELAY_MIN_SECONDS = 0
RETRY_DELAY_MAX_SECONDS = 5

# constants for Genie Table fields
GENIE_TABLE_NAME = 'name'
GENIE_TABLE_CATEGORY = 'category'
GENIE_TABLE_FIELDS = 'fields'
GENIE_TABLE_DISPLAY_NAME = 'displayName'
GENIE_TABLE_RELATIONSHIPS = 'relationships'
GENIE_TABLE_DIMENSIONS = 'dimensions'
GENIE_TABLE_MEASURES = 'measures'
GENIE_TABLE_PARTITION_BY = 'partitionBy'
GENIE_TABLE_PRIMARY_KEYS = 'primaryKeys'
GENIE_TABLE_INDEXES = 'indexes'
PRIMARY_KEY_NAME = 'name'
PRIMARY_KEY_DISPLAY_NAME = 'displayName'
PRIMARY_KEY_INDEX_ORDER = 'indexOrder'
RELATIONSHIP_FROM_TABLE = 'fromEntity'
RELATIONSHIP_TO_TABLE = 'toEntity'
RELATIONSHIP_FROM_ENTITY_ATTRIBUTE = 'fromEntityAttribute'
RELATIONSHIP_TO_ENTITY_ATTRIBUTE = 'toEntityAttribute'
RELATIONSHIP_CARDINALITY = 'cardinality'
FIELDS_NAME = 'name'
FIELDS_DISPLAY_NAME = 'displayName'
FIELDS_TYPE = 'type'
