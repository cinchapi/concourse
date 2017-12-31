# Copyright (c) 2013-2017 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This file contains shared definitions that are unlikely to be modified
# post thrift generation.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java interface/shared.thrift
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py interface/shared.thrift
namespace py concourse.thriftapi.shared

# To generate PHP source code run:
# thrift -out concourse-driver-php/src -gen php interface/shared.thrift
namespace php concourse.thrift.shared

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/ -gen rb:namespaced interface/shared.thrift
namespace rb concourse.thrift

/**
 * Enumerates the list of operators that can be used in criteria specifications.
 */
enum Operator {
  REGEX = 1,
  NOT_REGEX = 2,
  EQUALS = 3,
  NOT_EQUALS = 4,
  GREATER_THAN = 5,
  GREATER_THAN_OR_EQUALS = 6,
  LESS_THAN = 7,
  LESS_THAN_OR_EQUALS = 8,
  BETWEEN = 9,
  LINKS_TO = 10,
  LIKE = 11,
  NOT_LIKE = 12
}

/**
 * Enumerates the possible TObject types
 */
enum Type {
  BOOLEAN = 1,
  DOUBLE = 2,
  FLOAT = 3,
  INTEGER = 4,
  LONG = 5,
  LINK = 6,
  STRING = 7,
  TAG = 8,
  NULL = 9,
  TIMESTAMP = 10,
}

/** When re-constructing the state of a record/field/index from some base state,
 * A {@link Diff} describes the {@link Action} necessary to perform using the
 * data from a {@link Write}.
 */
enum Diff {
  ADDED = 1,
  REMOVED = 2,
}

/**
 * A temporary token that is returned by the
 * {@link ConcourseService#login(String, String)} method to grant access
 * to secure resources in place of raw credentials.
 */
struct AccessToken {
  1:required binary data
}

/**
 * A token that identifies a Transaction.
 */
struct TransactionToken {
  1:required AccessToken accessToken
  2:required i64 timestamp
}
