#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

class QueryResult:

    def __init__(self, data, description, has_next, next_batch_id):
        self.data = data
        self.description = description
        self.has_next = has_next
        self.next_batch_id = next_batch_id
