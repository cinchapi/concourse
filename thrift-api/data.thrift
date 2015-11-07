# This file contains "data" definitions that likely have been modified
# post thrift generation. Therefore, it is advisable to compare any
# regenerated versions of these structs with previous versions to
# make sure that any additional custom logic is retained.

include "shared.thrift"

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java thrift-api/data.thrift
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py thrift-api/data.thrift
namespace py concourse.thriftapi.data

# To generate PHP source code run:
# thrift -out concourse-driver-php/src -gen php thrift-api/data.thrift
namespace php concourse.thrift.data

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/ -gen rb:namespaced thrift-api/data.thrift
namespace rb concourse.thrift

/**
 * A lightweight wrapper for a typed Object that has been encoded
 * as binary data.
 */
struct TObject {
  1:required binary data,
  2:required shared.Type type = shared.Type.STRING
}

/**
 * A representation for an enum that declares the type of a TSymbol.
 */
enum TSymbolType {
  CONJUNCTION = 1,
  KEY = 2,
  VALUE = 3,
  PARENTHESIS = 4,
  OPERATOR = 5,
  TIMESTAMP = 6
}

/**
 * A representation for a Symbol that can be passed over the wire via
 * Thrift. Once passed over the wire, the server uses information
 * about the symbol type to parse the string representation of the
 * symbol to an actual object.
 */
struct TSymbol {
  1:required TSymbolType type;
  2:required string symbol;
}

/**
 * A representation for a Criteria that can be passed over the wire via
 * Thrift. Once passed over the write, the server goes through the list
 * of TSymbols and converts them to actual Symbol objects which can then
 * be used in the shunting-yard algorithm.
 */
struct TCriteria {
  1:required list<TSymbol> symbols
}
