# This file contains "data" definitions that likely have been modified
# post thrift generation. Therefore, it is advisable to compare any
# regenerated versions of these structs with previous versions to
# make sure that any additional custom logic is retained.
#
# To generate java source code run:
# thrift -out ../../java -gen java data.thrift

include "shared.thrift" 

namespace java org.cinchapi.concourse.thrift

/**
 * A lightweight wrapper for a typed Object that has been encoded 
 * as binary data.
 */	
struct TObject {
	1:required binary data,
	2:required shared.Type type = shared.Type.STRING
}