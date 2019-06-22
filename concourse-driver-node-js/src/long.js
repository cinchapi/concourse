'use strict'

const Int64 = require('node-int64')
const Int64Util = require('thrift/lib/nodejs/lib/thrift/int64_util')

const Long = function Long(...args) {
  if (! Long.isLong(this)) {
    return new Long(...args)
  }
  Int64.call(this, ...args)
}

Long.prototype = Object.create(Int64.prototype)
Long.prototype.constructor = Long

Long.fromNumberString = function fromNumberString(string) {
  const long = Int64Util.fromDecimalString(string)
  return Long(long.buffer)
}

Long.isLong = function isLong(value) {
  return value instanceof Long
}

Long.toNumberString = function toNumberString(long) {
  return Int64Util.toDecimalString(long)
}

// Long.prototype.toJSON = function toJSON() {
//   return Long.toNumberString(this)
// }

exports = module.exports = Long
