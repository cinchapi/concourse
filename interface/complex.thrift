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
# This file contains "complex" data structures that build on those defined
# in data.thrift.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

include "data.thrift"

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java interface/complex.thrift
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py interface/complex.thrift
namespace py concourse.thriftapi.complex

# To generate PHP source code run:
# thrift -out concourse-driver-php/src -gen php interface/complex.thrift
namespace php concourse.thrift.complex

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/ -gen rb:namespaced interface/complex.thrift
namespace rb concourse.thrift

/**
 * The possible types for a {@link ComplexTObject}.
 */
enum ComplexTObjectType {
    SCALAR = 1,
    MAP = 2,
    LIST = 3,
    SET = 4,
    TOBJECT = 5,
    TCRITERIA = 6,
    BINARY = 7
}

/**
 * A recursive structure that encodes one or more {@link TObject TObjects}.
 *
 * <p>
 * The most basic {@link ComplexTObject} is a
 * {@link ComplexTObjectType#SCALAR scalar}, which is just a wrapped
 * {@link TObject}. Beyond that, complex collections can be represented as a
 * {@link Set}, {@link List} or {@link Map} of
 * {@link ComplexTObject ComplexTObjects}.
 * </p>
 */
struct ComplexTObject {
    1: required ComplexTObjectType type,
    2: optional data.TObject tscalar,
    3: optional map<ComplexTObject, ComplexTObject> tmap,
    4: optional list<ComplexTObject> tlist,
    5: optional set<ComplexTObject> tset,
    6: optional data.TObject tobject,
    7: optional data.TCriteria tcriteria,
    8: optional binary tbinary
}
